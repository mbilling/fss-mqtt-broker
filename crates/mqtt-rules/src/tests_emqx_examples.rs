//! EMQX's documented built-in function examples, run against this engine.
//!
//! Every row is an example from EMQX's function reference,
//! `en_US/develop/data-integration/rule-sql-builtin-functions.md` (emqx-docs,
//! `release-6.2`), and each table carries the reference's `##` section it covers. A row's
//! `doc` names the `###` heading the example sits under as `name/arity`
//! (`### abs(X: integer) -> integer` is `abs/1`; `/N` is variadic). The reference writes
//! examples as `expr = value`; a row's value is the exact JSON this engine renders for
//! the expression, which equals EMQX's value under JSON equality with integers and
//! floats kept apart (`3.0` is not `3`). A `# Wrong` example is a row that must fail,
//! with the exact error the rule's failure counter records.
//!
//! Each row runs as `SELECT <expr> AS r FROM "#"` on one message: topic
//! `devices/A_C001/data` from client `c_emqx` / user `u_emqx` at `127.0.0.1:52000`, at
//! `QoS` 1 with retain set, with the row's payload (`{}` unless the example reads one).
//!
//! Where a row's value is not the reference's text it says why, next to the row:
//!
//! - **a typo in the reference**: the row asserts the value the reference's own prose
//!   (or EMQX's source) gives, and names the typo;
//! - **a last-digit difference** in a transcendental function: Erlang's libm and Rust's
//!   round the last bit differently, so the row asserts EMQX's value within 1e-12
//!   relative;
//! - **a documented difference**: `is_empty` of `undefined`.
//!
//! An example that writes C escapes inside a literal (`'\t  hello  \n'`) runs through
//! `unescape`, as the reference's own tip says it must: a rule SQL string literal has no
//! escapes, in EMQX's grammar or this one.
//!
//! A function or arity the reference gives no example for (the `is_not_null` pair, `map`,
//! `map_new`, the legacy accessors EMQX keeps undocumented, `join_to_string/1`, `mget/3`
//! and the list forms of `coalesce` and `coalesce_ne`) has rows derived from EMQX's
//! source, `apps/emqx_rule_engine/src/emqx_rule_funcs.erl`; their `doc` reads
//! `emqx_rule_funcs.erl name/arity`. Values that change on every call (`random()`, the
//! clock, UUIDs, message ids, the local time zone) are checked for the exact form the
//! reference shows, against the wall clock read around the call.
//! [`every_implemented_function_has_an_example`] keeps every function the engine
//! implements in some row.

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::TimeZone as _;

use super::*;

/// One documented example.
#[derive(Debug, Clone, Copy)]
struct Ex {
    /// The reference's `###` heading as `name/arity`, or `emqx_rule_funcs.erl name/arity`
    /// for a row derived from EMQX's source.
    doc: &'static str,
    /// The expression, run as `SELECT <expr> AS r FROM "#"`.
    expr: &'static str,
    /// The message's payload.
    payload: &'static str,
    want: Want,
}

/// What the expression must give.
#[derive(Debug, Clone, Copy)]
enum Want {
    /// Exactly this JSON text.
    Json(&'static str),
    /// A float within 1e-12 relative of EMQX's printed value (a last-digit libm
    /// difference).
    Near(&'static str),
    /// The SQL fails with exactly this error.
    Fails(&'static str),
    /// A value that changes on every call, in the form the reference shows.
    Shape(fn(&str, Window) -> Result<(), String>),
}

/// The wall clock just before and just after one evaluation, in ns since the epoch.
#[derive(Debug, Clone, Copy)]
struct Window {
    from_ns: i128,
    to_ns: i128,
}

const fn eq(doc: &'static str, expr: &'static str, json: &'static str) -> Ex {
    Ex {
        doc,
        expr,
        payload: "{}",
        want: Want::Json(json),
    }
}

const fn near(doc: &'static str, expr: &'static str, emqx: &'static str) -> Ex {
    Ex {
        doc,
        expr,
        payload: "{}",
        want: Want::Near(emqx),
    }
}

const fn fails(doc: &'static str, expr: &'static str, error: &'static str) -> Ex {
    Ex {
        doc,
        expr,
        payload: "{}",
        want: Want::Fails(error),
    }
}

const fn shape(
    doc: &'static str,
    expr: &'static str,
    check: fn(&str, Window) -> Result<(), String>,
) -> Ex {
    Ex {
        doc,
        expr,
        payload: "{}",
        want: Want::Shape(check),
    }
}

/// `ex` on a message with `payload`.
const fn on(payload: &'static str, ex: Ex) -> Ex {
    Ex { payload, ..ex }
}

// --- rule-sql-builtin-functions.md § Mathematical Functions -------------------------

const MATH: &[Ex] = &[
    eq("abs/1", "abs(-12)", "12"),
    // Last digit: EMQX prints ...976, mqttd ...979.
    near("acos/1", "acos(0.5)", "1.0471975511965976"),
    eq("acosh/1", "acosh(1.5)", "0.9624236501192069"),
    // Last digit: EMQX prints ...988, mqttd ...989.
    near("asin/1", "asin(0.5)", "0.5235987755982988"),
    eq("asinh/1", "asinh(0.5)", "0.48121182505960347"),
    // Last digit: EMQX prints ...0615, mqttd ...061.
    near("atan/1", "atan(0.5)", "0.46364760900080615"),
    // Last digit: EMQX prints ...549, mqttd ...548.
    near("atanh/1", "atanh(0.5)", "0.5493061443340549"),
    eq("ceil/1", "ceil(0.8)", "1"),
    eq("cos/1", "cos(0.5)", "0.8775825618903728"),
    eq("cosh/1", "cosh(0.5)", "1.1276259652063807"),
    eq("exp/1", "exp(1)", "2.718281828459045"),
    eq("floor/1", "floor(3.6)", "3"),
    eq("fmod/2", "fmod(6.5, 2.5)", "1.5"),
    eq("log/1", "log(7.38905609893065)", "2.0"),
    eq("log10/1", "log10(100)", "2.0"),
    eq("log2/1", "log2(8)", "3.0"),
    eq("log2/1", "log2(8.5)", "3.0874628412503395"),
    eq("round/1", "round(4.5)", "5"),
    eq("power/2", "power(2, 3)", "8.0"),
    // The reference shows one draw (0.5400050092601868) from `[0, 1)`.
    shape("random/0", "random()", unit_interval_float),
    eq("sin/1", "sin(0.5)", "0.479425538604203"),
    eq("sinh/1", "sinh(0.5)", "0.5210953054937474"),
    eq("sqrt/1", "sqrt(9)", "3.0"),
    eq("tan/1", "tan(0.5)", "0.5463024898437905"),
    eq("tanh/1", "tanh(0.5)", "0.46211715726000974"),
];

// --- rule-sql-builtin-functions.md § Data Type Judgment Functions --------------------

const TYPE_JUDGMENT: &[Ex] = &[
    eq("is_array/1", "is_array([1, 2])", "true"),
    eq(
        "is_array/1",
        r#"is_array(json_decode('[{"value": 1}]'))"#,
        "true",
    ),
    eq(
        "is_array/1",
        r#"is_array(json_decode('{"value": 1}'))"#,
        "false",
    ),
    eq("is_array/1", "is_array(0.5)", "false"),
    eq("is_array/1", "is_array('[1, 2]')", "false"),
    eq("is_bool/1", "is_bool(true)", "true"),
    // Typo: the reference says `is_bool(false) = false`. `false` is a boolean; EMQX's
    // source is `is_bool(T) when is_boolean(T) -> true`.
    eq("is_bool/1", "is_bool(false)", "true"),
    eq("is_bool/1", "is_bool('true')", "false"),
    eq("is_float/1", "is_float(123.4)", "true"),
    eq("is_float/1", "is_float(123)", "false"),
    eq("is_int/1", "is_int(123)", "true"),
    eq("is_int/1", "is_int(123.4)", "false"),
    eq("is_map/1", r#"is_map(json_decode('{"value": 1}'))"#, "true"),
    eq(
        "is_map/1",
        r#"is_map(json_decode('[{"value": 1}]'))"#,
        "false",
    ),
    eq(
        "is_null/1",
        "is_null(this_is_an_unassigned_variable)",
        "true",
    ),
    eq(
        "is_null/1",
        r#"is_null(map_get('b', json_decode('{"a": 1}')))"#,
        "true",
    ),
    eq(
        "is_null/1",
        r#"is_null(map_get('b', json_decode('{"b": null}')))"#,
        "false",
    ),
    eq(
        "is_null_var/1",
        "is_null_var(this_is_an_unassigned_variable)",
        "true",
    ),
    eq(
        "is_null_var/1",
        r#"is_null_var(map_get('b', json_decode('{"a": 1}')))"#,
        "true",
    ),
    eq(
        "is_null_var/1",
        r#"is_null_var(map_get('b', json_decode('{"b": null}')))"#,
        "true",
    ),
    // No examples in the reference ("the inverse of is_null_var"). EMQX's source:
    // `is_not_null(Data) -> not is_null(Data).` and
    // `is_not_null_var(Data) -> not is_null_var(Data).`: the examples above, negated.
    eq(
        "emqx_rule_funcs.erl is_not_null/1",
        "is_not_null(this_is_an_unassigned_variable)",
        "false",
    ),
    eq(
        "emqx_rule_funcs.erl is_not_null/1",
        r#"is_not_null(map_get('b', json_decode('{"b": null}')))"#,
        "true",
    ),
    eq(
        "emqx_rule_funcs.erl is_not_null_var/1",
        "is_not_null_var(this_is_an_unassigned_variable)",
        "false",
    ),
    eq(
        "emqx_rule_funcs.erl is_not_null_var/1",
        r#"is_not_null_var(map_get('b', json_decode('{"b": null}')))"#,
        "false",
    ),
    eq(
        "emqx_rule_funcs.erl is_not_null_var/1",
        r#"is_not_null_var(map_get('b', json_decode('{"b": 1}')))"#,
        "true",
    ),
    eq("is_num/1", "is_num(123)", "true"),
    eq("is_num/1", "is_num(123.4)", "true"),
    eq("is_num/1", "is_num('123')", "false"),
    eq("is_str/1", "is_str('123')", "true"),
    eq("is_str/1", "is_str(123)", "false"),
    eq("is_empty/1", "is_empty(json_decode('{}'))", "true"),
    eq("is_empty/1", "is_empty('{}')", "true"),
    eq("is_empty/1", r#"is_empty('{"key" : 1}')"#, "false"),
    eq(
        "is_empty/1",
        r#"is_empty(map_get('key', '{"key" : []}'))"#,
        "true",
    ),
    // A documented difference (docs/RULES.md, Differences from EMQX): the reference says
    // `false`. `'{"key" : [1}'` is not JSON,
    // so `map_get` gives `undefined`, and `is_empty(undefined)` fails here. It fails in
    // EMQX's source too: `is_empty/1` falls through to `map_size/1`, whose `map/1` is
    // `error(badarg)` for anything but a binary, list or map.
    fails(
        "is_empty/1",
        r#"is_empty(map_get('key', '{"key" : [1}'))"#,
        "is_empty(): expected an array or a map, got a undefined",
    ),
];

// --- rule-sql-builtin-functions.md § Data Type Conversion Functions ------------------

const CONVERSION: &[Ex] = &[
    eq("bool/1", "bool(true)", "true"),
    eq("bool/1", "bool(0)", "false"),
    eq("bool/1", "bool('false')", "false"),
    fails(
        "bool/1",
        "bool(20)",
        "bool(): cannot convert 20 to a boolean",
    ),
    fails(
        "bool/1",
        "bool('True')",
        "bool(): cannot convert True to a boolean",
    ),
    eq("float/1", "float(20)", "20.0"),
    eq("float/1", "float('3.14')", "3.14"),
    // The reference writes these as `31400` (an integer) and `0.000314`. `float/1` is
    // `emqx_utils_conv:float/1` (emqx_rule_funcs.erl `float(Data)`), which returns an
    // Erlang float, and jiffy writes a float in Erlang's shortest form: `3.14e4` and
    // `3.14e-4` (EMQX 6.3.1), the same numbers.
    eq("float/1", "float('3.14e4')", "3.14e4"),
    eq("float/1", "float('3.14e+4')", "3.14e4"),
    eq("float/1", "float('3.14e-4')", "3.14e-4"),
    eq("float/1", "float('3.14E-4')", "3.14e-4"),
    eq(
        "float/1",
        "float('0.12345678901234566')",
        "0.12345678901234566",
    ),
    eq(
        "float/1",
        "float('0.12345678901234567')",
        "0.12345678901234566",
    ),
    eq("float/2", "float('3.1415926', 3)", "3.142"),
    // The reference's `0.00001`, which jiffy writes as `1.0e-5`.
    eq("float/2", "float('0.000012345', 5)", "1.0e-5"),
    eq("float2str/2", "float2str(0.1, 5)", r#""0.1""#),
    eq(
        "float2str/2",
        "float2str(0.1, 20)",
        r#""0.10000000000000000555""#,
    ),
    eq(
        "float2str/2",
        "float2str(0.1, 25)",
        r#""0.1000000000000000055511151""#,
    ),
    eq(
        "float2str/2",
        "float2str(0.00000000001, 20)",
        r#""0.00000000001""#,
    ),
    eq("float2str/2", "float2str(0.100001, 5)", r#""0.1""#),
    eq(
        "float2str/2",
        "float2str(123456789.01234565, 8)",
        r#""123456789.01234566""#,
    ),
    eq(
        "float2str/2",
        "float2str(123456789.01234566, 8)",
        r#""123456789.01234566""#,
    ),
    eq("int/1", "int(true)", "1"),
    eq("int/1", "int(3.14)", "3"),
    // Typo: the reference says `int(-3.14) = 4`, dropping the sign. Its own prose: a
    // float is "rounded down ... the largest integer less than or equal to Term".
    eq("int/1", "int(-3.14)", "-4"),
    eq("int/1", "int('-100')", "-100"),
    eq("int/1", "int('+200')", "200"),
    eq("int/1", "int('0010')", "10"),
    eq("int/1", "int('3.1415e2')", "314"),
    eq("int/1", "int(substr('Number 100', 7))", "100"),
    fails(
        "int/1",
        "int('-100+200')",
        "int(): cannot convert '-100+200' to an integer",
    ),
    fails(
        "int/1",
        "int('Number 100')",
        "int(): cannot convert 'Number 100' to an integer",
    ),
    eq("str/1", "str(100)", r#""100""#),
    eq("str/1", "str(nth(1, json_decode('[false]')))", r#""false""#),
    // Typo: the reference writes `json_decode({"msg": "hello"})`, the JSON unquoted,
    // which is not SQL in either grammar. Quoted, as in the example after it.
    eq(
        "str/1",
        r#"str(json_decode('{"msg": "hello"}'))"#,
        r#""{\"msg\":\"hello\"}""#,
    ),
    eq(
        "str/1",
        r#"str(json_decode('[{"msg": "hello"}]'))"#,
        r#""[{\"msg\":\"hello\"}]""#,
    ),
    eq("str/1", "str(0.30000000040)", r#""0.3000000004""#),
    eq("str/1", "str(0.30000000004)", r#""0.3""#),
    eq("str/1", "str(3.14159265359)", r#""3.1415926536""#),
    eq("str/1", "str(0.000000314159265359)", r#""0.0000003142""#),
    eq("str_utf8/1", "str_utf8(100)", r#""100""#),
    eq(
        "str_utf8/1",
        "str_utf8(nth(1, json_decode('[false]')))",
        r#""false""#,
    ),
    // Typo: unquoted JSON, as for `str/1` above.
    eq(
        "str_utf8/1",
        r#"str_utf8(json_decode('{"msg": "hello"}'))"#,
        r#""{\"msg\":\"hello\"}""#,
    ),
    eq(
        "str_utf8/1",
        r#"str_utf8(json_decode('[{"msg": "hello"}]'))"#,
        r#""[{\"msg\":\"hello\"}]""#,
    ),
    eq("str_utf8/1", "str_utf8(0.30000000040)", r#""0.3000000004""#),
    eq("str_utf8/1", "str_utf8(0.30000000004)", r#""0.3""#),
    eq("str_utf8/1", "str_utf8(3.14159265359)", r#""3.1415926536""#),
    eq(
        "str_utf8/1",
        "str_utf8(0.000000314159265359)",
        r#""0.0000003142""#,
    ),
    eq("str_utf16_le/1", "str_utf16_le('h')", r#""h\u0000""#),
    eq(
        "str_utf16_le/1",
        "bin2hexstr(str_utf16_le('hello'))",
        r#""680065006C006C006F00""#,
    ),
    // `map/1` is not in the reference. EMQX's source: `map(Bin)` decodes JSON and must
    // get an object, `map(Map)` is the map, and anything else is `error(badarg)`.
    eq(
        "emqx_rule_funcs.erl map/1",
        r#"map('{"a": 1}')"#,
        r#"{"a":1}"#,
    ),
    eq(
        "emqx_rule_funcs.erl map/1",
        r#"map(json_decode('{"a": 1}'))"#,
        r#"{"a":1}"#,
    ),
    fails(
        "emqx_rule_funcs.erl map/1",
        "map('[1]')",
        "map(): expected a map or a JSON object, got a array",
    ),
    fails(
        "emqx_rule_funcs.erl map/1",
        "map(1)",
        "map(): expected a map or a JSON object, got a integer",
    ),
];

// --- rule-sql-builtin-functions.md § String Operation Functions ----------------------

/// The newline-separated payload of the reference's `unescape` example.
const DEVICE_INFO: &str = "32A48702-1FA6-4E7C-97F7-8EA3EA48E8A3\n87.2\n12.3\nmy-device";

const STRINGS: &[Ex] = &[
    eq("ascii/1", "ascii('a')", "97"),
    eq("ascii/1", "ascii('abc')", "97"),
    eq("concat/2", "concat('Name:', 'John')", r#""Name:John""#),
    eq(
        "find/2",
        "find('..., Value: 1.2', 'Value:')",
        r#""Value: 1.2""#,
    ),
    eq("find/2", "find('..., Value: 1.2', 'Data')", r#""""#),
    eq(
        "find/3",
        "find('Front, Middle, End', ', ', 'leading')",
        r#"", Middle, End""#,
    ),
    eq(
        "find/3",
        "find('Front, Middle, End', ', ', 'trailing')",
        r#"", End""#,
    ),
    eq(
        "join_to_string/2",
        "join_to_string(', ', ['a', 'b', 'c'])",
        r#""a, b, c""#,
    ),
    // The reference documents only the two-argument form. EMQX's source:
    // `join_to_string(Str) -> emqx_variform_bif:join_to_string(Str).`, and
    // emqx_variform_bif.erl `join_to_string(List) -> join_to_string(<<", ">>, List).`
    eq(
        "emqx_rule_funcs.erl join_to_string/1",
        "join_to_string(['a', 'b'])",
        r#""a, b""#,
    ),
    eq("lower/1", "lower('Hello')", r#""hello""#),
    eq(
        "ltrim/1",
        r"ltrim(unescape('\t  hello  \n'))",
        r#""hello  \n""#,
    ),
    // Typo: the reference shows `'hello  \r\n'`, two spaces, for an input with one.
    eq(
        "ltrim/1",
        r"ltrim(unescape('\t  hello \r\n'))",
        r#""hello \r\n""#,
    ),
    eq("pad/2", "pad('hello', 8)", r#""hello   ""#),
    eq("pad/3", "pad('hello', 8, 'leading')", r#""   hello""#),
    eq("pad/3", "pad('hello', 8, 'trailing')", r#""hello   ""#),
    eq("pad/3", "pad('hello', 8, 'both')", r#"" hello  ""#),
    eq("pad/4", "pad('hello', 8, 'trailing', '!')", r#""hello!!!""#),
    eq(
        "pad/4",
        r"pad('hello', 8, 'trailing', unescape('\r\n'))",
        r#""hello\r\n\r\n\r\n""#,
    ),
    eq(
        "pad/4",
        "pad('hello', 8, 'trailing', 'abc')",
        r#""helloabcabcabc""#,
    ),
    eq("regex_match/2", r"regex_match('123', '^\d+$')", "true"),
    eq("regex_match/2", r"regex_match('a23', '^\d+$')", "false"),
    eq(
        "regex_replace/3",
        r"regex_replace('hello 123', '\d+', 'world')",
        r#""hello world""#,
    ),
    eq(
        "regex_replace/3",
        r"regex_replace('a;b; c', ';\s*', ',')",
        r#""a,b,c""#,
    ),
    eq(
        "regex_extract/2",
        r"regex_extract('Number: 12345', '(\d+)')",
        r#"["12345"]"#,
    ),
    eq(
        "regex_extract/2",
        r"regex_extract('Hello, world!', '(\w+).*\s(\w+)')",
        r#"["Hello","world"]"#,
    ),
    eq(
        "regex_extract/2",
        r"regex_extract('No numbers here!', '(\d+)')",
        "[]",
    ),
    eq(
        "regex_extract/2",
        r"regex_extract('Date: 2021-05-20', '(\d{4})-(\d{2})-(\d{2})')",
        r#"["2021","05","20"]"#,
    ),
    eq(
        "replace/3",
        "replace('ab..cd..ef', '..', '**')",
        r#""ab**cd**ef""#,
    ),
    eq(
        "replace/3",
        "replace('ab..cd..ef', '..', '')",
        r#""abcdef""#,
    ),
    eq(
        "replace/4",
        "replace('ab..cd..ef', '..', '**', 'all')",
        r#""ab**cd**ef""#,
    ),
    eq(
        "replace/4",
        "replace('ab..cd..ef', '..', '**', 'leading')",
        r#""ab**cd..ef""#,
    ),
    eq(
        "replace/4",
        "replace('ab..cd..ef', '..', '**', 'trailing')",
        r#""ab..cd**ef""#,
    ),
    eq("reverse/1", "reverse('hello')", r#""olleh""#),
    eq("rm_prefix/2", "rm_prefix('foo/bar', 'foo/')", r#""bar""#),
    eq(
        "rm_prefix/2",
        "rm_prefix('foo/bar', 'xxx/')",
        r#""foo/bar""#,
    ),
    eq(
        "rtrim/1",
        r"rtrim(unescape('\t  hello  \n'))",
        r#""\t  hello""#,
    ),
    eq(
        "rtrim/1",
        r"rtrim(unescape('\t  hello \r\n'))",
        r#""\t  hello""#,
    ),
    eq(
        "emqx_rule_funcs.erl rtrim/2",
        "rtrim('abcxxyx', 'xy')",
        r#""abc""#,
    ),
    eq("split/2", "split('a;', ';')", r#"["a"]"#),
    eq("split/2", "split('a;b;c', ';')", r#"["a","b","c"]"#),
    eq("split/2", "split('a;;b;;c', ';')", r#"["a","b","c"]"#),
    eq(
        "split/2",
        "split('Sienna Blake; Howell Wise', ';')",
        r#"["Sienna Blake"," Howell Wise"]"#,
    ),
    eq(
        "split/2",
        "split('Sienna Blake; Howell Wise', '; ')",
        r#"["Sienna Blake","Howell Wise"]"#,
    ),
    eq(
        "split/3",
        "split('a;;b;;c', ';', 'notrim')",
        r#"["a","","b","","c"]"#,
    ),
    eq(
        "split/3",
        "split('a;b;c', ';', 'leading')",
        r#"["a","b;c"]"#,
    ),
    eq(
        "split/3",
        "split('a;b;c', ';', 'trailing')",
        r#"["a;b","c"]"#,
    ),
    eq(
        "split/3",
        "split(';a;b;c', ';', 'leading_notrim')",
        r#"["","a;b;c"]"#,
    ),
    eq(
        "split/3",
        "split('a;b;c;', ';', 'trailing_notrim')",
        r#"["a;b;c",""]"#,
    ),
    eq(
        "sprintf/N",
        "sprintf('hello, ~s!', 'steve')",
        r#""hello, steve!""#,
    ),
    eq(
        "sprintf/N",
        "sprintf('count: ~p~n', 100)",
        r#""count: 100\n""#,
    ),
    eq("strlen/1", "strlen('hello')", "5"),
    eq("strlen/1", r"strlen(unescape('hello\n'))", "6"),
    eq("substr/2", "substr('hello', 0)", r#""hello""#),
    eq("substr/2", "substr('hello world', 6)", r#""world""#),
    eq("substr/3", "substr('hello world!', 6, 5)", r#""world""#),
    eq(
        "tokens/2",
        "tokens('a,b;c,d', ',;')",
        r#"["a","b","c","d"]"#,
    ),
    eq("tokens/2", "tokens('a;;b', ';')", r#"["a","b"]"#),
    eq(
        "tokens/3",
        r"tokens(unescape('a\rb\nc\r\nd'), ';', 'nocrlf')",
        r#"["a","b","c","d"]"#,
    ),
    eq("trim/1", r"trim(unescape('\t  hello  \n'))", r#""hello""#),
    eq("trim/1", r"trim(unescape('\t  hello \r\n'))", r#""hello""#),
    // The reference's worked example: without `unescape`, `'\n'` is a backslash and an
    // `n`, so the newline-separated payload does not split.
    on(
        DEVICE_INFO,
        eq(
            "unescape/1",
            r"split(payload, '\n')",
            r#"["32A48702-1FA6-4E7C-97F7-8EA3EA48E8A3\n87.2\n12.3\nmy-device"]"#,
        ),
    ),
    on(
        DEVICE_INFO,
        eq(
            "unescape/1",
            r"split(payload, unescape('\n'))",
            r#"["32A48702-1FA6-4E7C-97F7-8EA3EA48E8A3","87.2","12.3","my-device"]"#,
        ),
    ),
    // The reference lists the escapes without examples; each one, with `\xH...` taking
    // "one or more hexadecimal digits" as a code point (U+1F600 here).
    eq(
        "unescape/1",
        r"unescape('\a\b\f\v\?\x41\x1F600')",
        "\"\\u0007\\b\\f\\u000B?A\u{1F600}\"",
    ),
    // `\'` cannot be written inside a SQL literal (the quote ends it), so the escapes
    // that name quotes and the backslash come from the payload.
    on(
        r#"{"s": "\\' \\\" \\\\ \\?"}"#,
        eq("unescape/1", "unescape(payload.s)", r#""' \" \\ ?""#),
    ),
    // "If an escape sequence is not recognized ... the function throws an exception."
    fails(
        "unescape/1",
        r"unescape('\q')",
        r"unescape(): unescape: unknown escape \q",
    ),
    // Typo: the reference says `upper('hello') = 'Hello'`. Its own prose: "converts
    // lowercase letters in a String to uppercase letters".
    eq("upper/1", "upper('hello')", r#""HELLO""#),
];

// --- rule-sql-builtin-functions.md § Map Operation Functions -------------------------

const MAPS: &[Ex] = &[
    eq(
        "map_get/2",
        r#"map_get('msg', json_decode('{"msg": "hello"}'))"#,
        r#""hello""#,
    ),
    eq(
        "map_get/2",
        r#"map_get('data', json_decode('{"msg": "hello"}'))"#,
        r#""undefined""#,
    ),
    eq(
        "map_get/3",
        r#"map_get('data', json_decode('{"msg": "hello"}'), '')"#,
        r#""""#,
    ),
    eq(
        "map_get/3",
        r#"map_get('value', json_decode('{"data": [1.2, 1.3]}'), [])"#,
        "[]",
    ),
    eq(
        "map_keys/1",
        r#"map_keys(json_decode('{"a": 1, "b": 2}'))"#,
        r#"["a","b"]"#,
    ),
    eq(
        "map_put/3",
        r#"map_get('b', map_put('b', 1, json_decode('{"a": 1}')))"#,
        "1",
    ),
    eq(
        "map_put/3",
        r#"map_get('a', map_put('a', 2, json_decode('{"a": 1}')))"#,
        "2",
    ),
    eq(
        "map_to_entries/1",
        r#"map_to_entries(json_decode('{"a": 1, "b": 2}'))"#,
        r#"[{"key":"a","value":1},{"key":"b","value":2}]"#,
    ),
    eq(
        "map_values/1",
        r#"map_values(json_decode('{"a": 1, "b": 2}'))"#,
        "[1,2]",
    ),
    eq(
        "mget/2",
        r#"mget('c', json_decode('{"a": {"b": 1}}'))"#,
        r#""undefined""#,
    ),
    // Typo: the reference says `json_decode(mget('a', ...)) = '{"b": 1}'`. The value it
    // shows is a string, so the call it means is `json_encode`, which renders the nested
    // map as `'{"b":1}'` (EMQX's `json_encode/1` is `emqx_utils_json:encode/1`, jiffy, which
    // writes no spaces). `json_decode` takes a string, and EMQX's
    // (`emqx_utils_json:decode(Data, [return_maps])`) fails on the map `mget` returns, as
    // this does. The rows after are what the example shows: one key reaches the nested
    // map.
    fails(
        "mget/2",
        r#"json_decode(mget('a', json_decode('{"a": {"b": 1}}')))"#,
        "json_decode(): expected a string, got a map",
    ),
    eq(
        "mget/2",
        r#"json_encode(mget('a', json_decode('{"a": {"b": 1}}')))"#,
        r#""{\"b\":1}""#,
    ),
    eq(
        "mget/2",
        r#"mget('a', json_decode('{"a": {"b": 1}}'))"#,
        r#"{"b":1}"#,
    ),
    eq(
        "mget/2",
        r#"mget(['a', 'b'], json_decode('{"a": {"b": 1}}'))"#,
        "1",
    ),
    // The reference documents only `mget/2`, which EMQX's source defines as
    // `mget(Key, Map) -> mget(Key, Map, undefined).`: `mget/3` returns its third argument
    // where `mget/2` returns `undefined`.
    eq(
        "emqx_rule_funcs.erl mget/3",
        r#"mget('x', json_decode('{"a": 1}'), 'dflt')"#,
        r#""dflt""#,
    ),
    eq(
        "emqx_rule_funcs.erl mget/3",
        r#"mget('a', json_decode('{"a": 1}'), 'dflt')"#,
        "1",
    ),
    eq(
        "mput/3",
        r#"mget(['a', 'b'], mput(['a', 'b'], 2, json_decode('{"a": {"b": 1}}')))"#,
        "2",
    ),
    eq(
        "mput/3",
        r#"mget(['a', 'b'], mput(['a', 'b'], 2, json_decode('{"c": 1}')))"#,
        "2",
    ),
    eq("map_size/1", "map_size(json_decode('{}'))", "0"),
    eq(
        "map_size/1",
        r#"map_size(json_decode('{"msg": "hello"}'))"#,
        "1",
    ),
    // `map_new()` has no section (the reference names it under `maptab_lookup/3` as a
    // way to build a row). EMQX's source: `map_new() -> #{}.`
    eq("emqx_rule_funcs.erl map_new/0", "map_new()", "{}"),
    eq(
        "emqx_rule_funcs.erl map_new/0",
        "map_put('a', 1, map_new())",
        r#"{"a":1}"#,
    ),
    // The reference's prose example: `{"a" : 1, "b": 2}` gives `HMSET name1 b 2 a 1`
    // ("the order of the fields in the map is non-deterministic": for a small map it is
    // keys descending). The list starts with EMQX's marker atom.
    eq(
        "map_to_redis_hset_args/1",
        r#"map_to_redis_hset_args(json_decode('{"a" : 1, "b": 2}'))"#,
        r#"["map_to_redis_hset_args","b","2","a","1"]"#,
    ),
];

// --- rule-sql-builtin-functions.md § Array Operation Functions -----------------------

const ARRAYS: &[Ex] = &[
    eq("contains/2", "contains(2, [1, 2, 3])", "true"),
    eq("contains/2", "contains(2.3, [1.8, 2.5, 2.0])", "false"),
    eq("contains/2", "contains('John', ['John', 'David'])", "true"),
    eq("contains/2", "contains([1, 2], [a, b, [1, 2]])", "true"),
    eq(
        "contains/2",
        r#"contains(json_decode('{"a": 1}'), [json_decode('{"a": 1}'), json_decode('{"b": 2}')])"#,
        "true",
    ),
    eq("first/1", "first(['John', 'David'])", r#""John""#),
    fails("first/1", "first([])", "first(): first([]) is undefined"),
    eq("last/1", "last(['John', 'David'])", r#""David""#),
    fails("last/1", "last([])", "last(): last([]) is undefined"),
    eq("length/1", "length([1,2,3,4])", "4"),
    eq("length/1", "length([])", "0"),
    eq("nth/2", "nth(1, [1,2,3])", "1"),
    fails(
        "nth/2",
        "nth(0, [1,2,3])",
        "nth(): nth() positions start at 1",
    ),
    fails(
        "nth/2",
        "nth(4, [1,2,3])",
        "nth(): nth(4) is past the end of a 3-element array",
    ),
    eq("sublist/2", "sublist(3, [1,2,3,4])", "[1,2,3]"),
    eq("sublist/2", "sublist(10, [1,2,3,4])", "[1,2,3,4]"),
    eq("sublist/3", "sublist(2, 10, [1,2,3,4])", "[2,3,4]"),
];

// --- rule-sql-builtin-functions.md § Hashing Functions -------------------------------

const HASHING: &[Ex] = &[
    eq(
        "md5/1",
        "md5('hello')",
        r#""5d41402abc4b2a76b9719d911017c592""#,
    ),
    eq(
        "sha/1",
        "sha('hello')",
        r#""aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d""#,
    ),
    eq(
        "sha256/1",
        "sha256('hello')",
        r#""2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824""#,
    ),
    eq("hash_to_range/3", "hash_to_range('A_C001', 0, 3)", "0"),
    // The reference gives this one without a value. The message's topic is
    // `devices/A_C001/data`, so it buckets the example above's `'A_C001'`.
    eq(
        "hash_to_range/3",
        "hash_to_range(nth(2, tokens(topic, '/')), 0, 3)",
        "0",
    ),
    eq("map_to_range/3", "map_to_range(7, 0, 3)", "3"),
    eq("map_to_range/3", "map_to_range('a', 0, 3)", "1"),
    // `hash/2` reaches every digest `crypto:hash/2` offers; values from EMQX 6.3.1.
    eq(
        "emqx_rule_funcs.erl hash/2",
        "hash('sha1', 'abc')",
        r#""a9993e364706816aba3e25717850c26c9cd0d89d""#,
    ),
    eq(
        "emqx_rule_funcs.erl hash/2",
        "hash('sha3_224', 'abc')",
        r#""e642824c3f8cf24ad09234ee7d3c766fc9a3a5168d0c94ad73b46fdf""#,
    ),
];

// --- rule-sql-builtin-functions.md § Bit Operation Functions -------------------------

const BITS: &[Ex] = &[
    eq("bitand/2", "bitand(10, 8)", "8"),
    eq("bitand/2", "bitand(-10, -8)", "-16"),
    eq("bitnot/1", "bitnot(10)", "-11"),
    eq("bitnot/1", "bitnot(-12)", "11"),
    eq("bitsl/2", "bitsl(8, 2)", "32"),
    eq("bitsl/2", "bitsl(-8, 2)", "-32"),
    eq("bitsr/2", "bitsr(8, 2)", "2"),
    eq("bitsr/2", "bitsr(8, 4)", "0"),
    eq("bitsr/2", "bitsr(-8, 2)", "-2"),
    eq("bitsr/2", "bitsr(-8, 6)", "-1"),
    eq("bitor/2", "bitor(10, 8)", "10"),
    eq("bitor/2", "bitor(-10, -8)", "-2"),
    eq("bitxor/2", "bitxor(10, 8)", "2"),
    eq("bitxor/2", "bitxor(-10, -8)", "14"),
];

// --- rule-sql-builtin-functions.md § Encoding and Decoding Functions -----------------

const ENCODING: &[Ex] = &[
    eq("base64_decode/1", "base64_decode('aGVsbG8=')", r#""hello""#),
    eq(
        "base64_decode/1",
        "bin2hexstr(base64_decode('y0jN'))",
        r#""CB48CD""#,
    ),
    // The option forms are shown as statements without outputs. The bytes FB FF encode
    // as `+/8=`, which the URL-safe alphabet (`-` for 62, `_` for 63) writes `-_8=`.
    on(
        "-_8=",
        eq(
            "base64_decode/N",
            "bin2hexstr(base64_decode(payload, 'urlsafe'))",
            r#""FBFF""#,
        ),
    ),
    on(
        "-_8",
        eq(
            "base64_decode/N",
            "bin2hexstr(base64_decode(payload, 'urlsafe', 'no_padding'))",
            r#""FBFF""#,
        ),
    ),
    eq("base64_encode/1", "base64_encode('hello')", r#""aGVsbG8=""#),
    eq(
        "base64_encode/1",
        "base64_encode(hexstr2bin('CB48CD'))",
        r#""y0jN""#,
    ),
    on(
        "hello",
        eq(
            "base64_encode/N",
            "base64_encode(payload, 'no_padding')",
            r#""aGVsbG8""#,
        ),
    ),
    eq(
        "base64_encode/N",
        "base64_encode(hexstr2bin('FBFF'), 'urlsafe')",
        r#""-_8=""#,
    ),
    eq(
        "base64_encode/N",
        "base64_encode(hexstr2bin('FBFF'), 'no_padding', 'urlsafe')",
        r#""-_8""#,
    ),
    eq(
        "json_decode/1",
        r#"map_get('a', json_decode('{"a": 1}'))"#,
        "1",
    ),
    eq("json_encode/1", "json_encode([1,2,3])", r#""[1,2,3]""#),
    eq(
        "bin2hexstr/1",
        "bin2hexstr(zip('hello'))",
        r#""CB48CDC9C90700""#,
    ),
    eq(
        "hexstr2bin/1",
        "unzip(hexstr2bin('CB48CDC9C90700'))",
        r#""hello""#,
    ),
    // EMQX's `hexstr_to_bin/1` reads an odd digit count as if it had a leading 0.
    eq(
        "emqx_rule_funcs.erl hexstr2bin/1",
        "bin2hexstr(hexstr2bin('abc'))",
        r#""0ABC""#,
    ),
    eq(
        "emqx_rule_funcs.erl bin2hexstr/2",
        "bin2hexstr('ab', '0x')",
        r#""0x6162""#,
    ),
    eq(
        "emqx_rule_funcs.erl bin2hexstr/2",
        "bin2hexstr('ab', payload.none)",
        r#""6162""#,
    ),
    eq(
        "emqx_rule_funcs.erl hexstr2bin/2",
        "hexstr2bin('0x6162', '0x')",
        r#""ab""#,
    ),
    fails(
        "emqx_rule_funcs.erl hexstr2bin/2",
        "hexstr2bin('6162', '0x')",
        "hexstr2bin(): the string does not start with '0x'",
    ),
    eq(
        "sqlserver_bin2hexstr/1",
        "sqlserver_bin2hexstr('hello')",
        r#""0x68656C6C6F""#,
    ),
    eq(
        "sqlserver_bin2hexstr/1",
        "sqlserver_bin2hexstr(str_utf16_le('hello'))",
        r#""0x680065006C006C006F00""#,
    ),
    eq(
        "sqlserver_bin2hexstr/1",
        "sqlserver_bin2hexstr(str_utf16_le('你好'))",
        r#""0x604F7D59""#,
    ),
];

// --- rule-sql-builtin-functions.md § Compression and Decompression Functions ---------

const COMPRESSION: &[Ex] = &[
    // The reference's gzip bytes were written on macOS (OS byte 0x13); EMQX's Linux
    // images write 03, as zlib does on Linux and as this engine does everywhere
    // (verified on emqx/emqx:6.3.1). Both decompress.
    eq(
        "gzip/1",
        "bin2hexstr(gzip('hello'))",
        r#""1F8B0800000000000003CB48CDC9C9070086A6103605000000""#,
    ),
    eq(
        "gunzip/1",
        "gunzip(hexstr2bin('1F8B0800000000000013CB48CDC9C9070086A6103605000000'))",
        r#""hello""#,
    ),
    eq("zip/1", "bin2hexstr(zip('hello'))", r#""CB48CDC9C90700""#),
    eq(
        "unzip/1",
        "unzip(hexstr2bin('CB48CDC9C90700'))",
        r#""hello""#,
    ),
    eq(
        "zip_compress/1",
        "bin2hexstr(zip_compress('hello'))",
        r#""789CCB48CDC9C90700062C0215""#,
    ),
    eq(
        "zip_uncompress/1",
        "zip_uncompress(hexstr2bin('789CCB48CDC9C90700062C0215'))",
        r#""hello""#,
    ),
    eq(
        "lz4_compress/1",
        "lz4_uncompress(lz4_compress('hello'))",
        r#""hello""#,
    ),
    eq(
        "lz4_uncompress/1",
        "lz4_uncompress(lz4_compress('hello'))",
        r#""hello""#,
    ),
];

// --- rule-sql-builtin-functions.md § Bit Sequence Operation Functions ----------------

const BIT_SEQUENCES: &[Ex] = &[
    eq("bitsize/1", "bitsize('abc')", "24"),
    eq("bitsize/1", "bitsize('你好')", "48"),
    // The reference's heading and examples spell it `byteszie`; EMQX's function, and
    // so this one, is `bytesize` (`byteszie` fails the load in both).
    eq("byteszie/1", "bytesize('abc')", "3"),
    eq("byteszie/1", "bytesize('你好')", "6"),
    eq("subbits/2", "subbits(hexstr2bin('9F4E58'), 8)", "159"),
    eq("subbits/2", "subbits(hexstr2bin('9F4E58'), 16)", "40782"),
    eq("subbits/2", "subbits(base64_decode('n05Y'), 8)", "159"),
    eq("subbits/3", "subbits(hexstr2bin('9F4E58'), 1, 8)", "159"),
    eq("subbits/3", "subbits(hexstr2bin('9F4E58'), 9, 8)", "78"),
    eq("subbits/3", "subbits(base64_decode('n05Y'), 9, 4)", "4"),
    eq(
        "subbits/6",
        "subbits(hexstr2bin('9F4E58'), 1, 16, 'integer', 'unsigned', 'big')",
        "40782",
    ),
    eq(
        "subbits/6",
        "subbits(hexstr2bin('9F4E58'), 1, 16, 'integer', 'signed', 'big')",
        "-24754",
    ),
    eq(
        "subbits/6",
        "subbits(hexstr2bin('9F4E58'), 1, 16, 'integer', 'unsigned', 'little')",
        "20127",
    ),
    eq(
        "subbits/6",
        "subbits(hexstr2bin('9F4E58'), 1, 16, 'float', 'unsigned', 'big')",
        "-0.00713348388671875",
    ),
    eq(
        "subbits/6",
        "subbits(hexstr2bin('9F4E58'), 1, 16, 'float', 'signed', 'big')",
        "-0.00713348388671875",
    ),
    // The 4- and 5-argument forms default the rest as EMQX's source does.
    eq(
        "emqx_rule_funcs.erl subbits/4",
        "bin2hexstr(subbits(hexstr2bin('9F4E58'), 9, 16, 'bits'))",
        r#""4E58""#,
    ),
    eq(
        "emqx_rule_funcs.erl subbits/5",
        "subbits(hexstr2bin('9F4E58'), 1, 8, 'integer', 'signed')",
        "-97",
    ),
];

// --- rule-sql-builtin-functions.md § System Function ---------------------------------

const SYSTEM: &[Ex] = &[
    // An unset variable reads as the empty string.
    eq("getenv/1", "getenv('MQTTD_TEST_NEVER_SET')", r#""""#),
];

// --- emqx_rule_funcs.erl: exported, so callable, but not in the reference ------------

const UNDOCUMENTED: &[Ex] = &[
    eq("emqx_rule_funcs.erl div/2", "div(7, 2)", "3"),
    eq("emqx_rule_funcs.erl div/2", "div(-7, 2)", "-3"),
    eq("emqx_rule_funcs.erl mod/2", "mod(-7, 2)", "-1"),
    fails(
        "emqx_rule_funcs.erl div/2",
        "div(7.0, 2)",
        "div(): expected an integer, got a float",
    ),
    eq("emqx_rule_funcs.erl eq/2", "eq(1, 1.0)", "true"),
    eq("emqx_rule_funcs.erl eq/2", "eq('1', 1)", "false"),
    eq("emqx_rule_funcs.erl null/0", "is_null(null())", "true"),
    // Topic filters match only as maps with the atom key `topic`, which no rule value
    // has: a list never contains the topic, and anything else fails.
    eq(
        "emqx_rule_funcs.erl contains_topic/2",
        r#"contains_topic(json_decode('[{"topic":"t/a","qos":1}]'), 't/a')"#,
        "false",
    ),
    eq(
        "emqx_rule_funcs.erl contains_topic/3",
        r#"contains_topic(json_decode('[{"topic":"t/a","qos":1}]'), 't/a', 1)"#,
        "false",
    ),
    eq(
        "emqx_rule_funcs.erl contains_topic_match/2",
        r#"contains_topic_match(json_decode('[{"topic":"t/#","qos":1}]'), 't/a')"#,
        "false",
    ),
    fails(
        "emqx_rule_funcs.erl contains_topic_match/3",
        "contains_topic_match('t/#', 't/a', 1)",
        "contains_topic_match(): expected an array, got a string",
    ),
    eq(
        "emqx_rule_funcs.erl join_to_sql_values_string/1",
        r#"join_to_sql_values_string(json_decode('["x\\y",1,1.5,true,null,[1,2]]'))"#,
        r#""'x\\\\y', 1, 1.5, 'true', 'null', '[1,2]'""#,
    ),
    eq(
        "emqx_rule_funcs.erl sprintf_s/2",
        "sprintf_s('~p-~p', [1, 2])",
        r#""1-2""#,
    ),
];

// --- rule-sql-builtin-functions.md § Date and Time Conversion Functions --------------

const TIME: &[Ex] = &[
    eq(
        "date_to_unix_ts/3",
        "date_to_unix_ts('second', '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 15:00:00+08:00')",
        "1708671600",
    ),
    eq(
        "date_to_unix_ts/4",
        "date_to_unix_ts('second', '+08:00', '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 15:00:00')",
        "1708671600",
    ),
    eq(
        "date_to_unix_ts/4",
        "date_to_unix_ts('second', 'Z', '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 07:00:00')",
        "1708671600",
    ),
    eq(
        "date_to_unix_ts/4",
        "date_to_unix_ts('second', 14400, '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 15:00:00')",
        "1708686000",
    ),
    eq(
        "format_date/4",
        "format_date('millisecond', '+08:00', '%Y-%m-%d %H:%M:%S.%6N%z', 1708933353472)",
        r#""2024-02-26 15:42:33.472000+0800""#,
    ),
    eq(
        "format_date/4",
        "format_date('millisecond', '+08:00', '%Y-%m-%d %H:%M:%S.%6N%:z', 1708933353472)",
        r#""2024-02-26 15:42:33.472000+08:00""#,
    ),
    eq(
        "format_date/4",
        "format_date('millisecond', '+08:20:30', '%Y-%m-%d %H:%M:%S.%3N%::z', 1708933353472)",
        r#""2024-02-26 16:03:03.472+08:20:30""#,
    ),
    // Typo: the reference shows `...07:42:33.472+08:00`. The time is UTC's (the
    // `+08:00` rows above show 15:42), and offset `Z` is UTC, so the zone is `+00:00`.
    eq(
        "format_date/4",
        "format_date('millisecond', 'Z', '%Y-%m-%d %H:%M:%S.%3N%:z', 1708933353472)",
        r#""2024-02-26 07:42:33.472+00:00""#,
    ),
    eq(
        "format_date/4",
        "format_date('millisecond', 28800, '%Y-%m-%d %H:%M:%S.%3N%:z', 1708933353472)",
        r#""2024-02-26 15:42:33.472+08:00""#,
    ),
    // The reference shows `'2024-02-23T10:26:20+08:00'`: whole seconds, local zone.
    shape("now_rfc3339/0", "now_rfc3339()", now_rfc3339_seconds),
    // The reference shows `'2024-02-23T10:26:38.009706+08:00'`.
    shape(
        "now_rfc3339/1",
        "now_rfc3339('microsecond')",
        now_rfc3339_micros,
    ),
    // The reference shows `1708913853`.
    shape("now_timestamp/0", "now_timestamp()", now_unix_seconds),
    // The reference shows `1708913828814315`.
    shape(
        "now_timestamp/1",
        "now_timestamp('microsecond')",
        now_unix_micros,
    ),
    eq(
        "rfc3339_to_unix_ts/1",
        "rfc3339_to_unix_ts('2024-02-23T15:56:30Z')",
        "1708703790",
    ),
    eq(
        "rfc3339_to_unix_ts/1",
        "rfc3339_to_unix_ts('2024-02-23T15:56:30+08:00')",
        "1708674990",
    ),
    eq(
        "rfc3339_to_unix_ts/2",
        "rfc3339_to_unix_ts('2024-02-23T15:56:30.87Z', 'second')",
        "1708703790",
    ),
    eq(
        "rfc3339_to_unix_ts/2",
        "rfc3339_to_unix_ts('2024-02-23T15:56:30.87Z', 'millisecond')",
        "1708703790870",
    ),
    eq(
        "rfc3339_to_unix_ts/2",
        "rfc3339_to_unix_ts('2024-02-23T15:56:30.87Z', 'microsecond')",
        "1708703790870000",
    ),
    eq(
        "rfc3339_to_unix_ts/2",
        "rfc3339_to_unix_ts('2024-02-23T15:56:30.535904509Z', 'nanosecond')",
        "1708703790535904509",
    ),
    eq(
        "timezone_to_offset_seconds/1",
        "timezone_to_offset_seconds('Z')",
        "0",
    ),
    eq(
        "timezone_to_offset_seconds/1",
        "timezone_to_offset_seconds('+08:00')",
        "28800",
    ),
    eq(
        "emqx_rule_funcs.erl timezone_to_second/1",
        "timezone_to_second('+08:00')",
        "28800",
    ),
    // `format_date/3` formats the time of the call.
    shape(
        "emqx_rule_funcs.erl format_date/3",
        "format_date('second', '+00:00', '%s')",
        now_unix_seconds_text,
    ),
    // The reference's 28800 is its writer's +08:00 zone: the offset of this host's.
    shape(
        "timezone_to_offset_seconds/1",
        "timezone_to_offset_seconds('local')",
        local_offset_seconds,
    ),
    // The reference shows `'2024-02-23T15:00:00+08:00'`: this instant in its writer's
    // zone. Here, the same instant in this host's.
    shape(
        "unix_ts_to_rfc3339/1",
        "unix_ts_to_rfc3339(1708671600)",
        documented_instant_seconds,
    ),
    // The reference shows `'2024-02-23T15:00:00.766+08:00'`.
    shape(
        "unix_ts_to_rfc3339/2",
        "unix_ts_to_rfc3339(1708671600766, 'millisecond')",
        documented_instant_millis,
    ),
];

// --- rule-sql-builtin-functions.md § UUID Functions ----------------------------------

const UUID: &[Ex] = &[
    // The reference shows `'f5bb7bea-a371-4df7-aa30-479add04632b'`.
    shape("uuid_v4/0", "uuid_v4()", uuid_v4_hyphenated),
    // The reference shows `'d7a39aa4195a42068b962eb9a665503e'`.
    shape("uuid_v4_no_hyphen/0", "uuid_v4_no_hyphen()", uuid_v4_bare),
];

// --- rule-sql-builtin-functions.md § Conditional Functions ---------------------------
//
// Prose, not `=` examples: `coalesce(payload.value, 0)` "returns payload.value if it is
// not null, or 0 if it is null", "equivalent to ... CASE WHEN is_null(payload.value)
// THEN 0 ELSE payload.value END"; `coalesce_ne` also replaces an empty string.

const CONDITIONAL: &[Ex] = &[
    eq("coalesce/2", "coalesce(payload.value, 0)", "0"),
    on(
        r#"{"value": 5}"#,
        eq("coalesce/2", "coalesce(payload.value, 0)", "5"),
    ),
    on(
        r#"{"value": 5}"#,
        eq(
            "coalesce/2",
            "CASE WHEN is_null(payload.value) THEN 0 ELSE payload.value END",
            "5",
        ),
    ),
    eq(
        "coalesce/2",
        "CASE WHEN is_null(payload.value) THEN 0 ELSE payload.value END",
        "0",
    ),
    eq("coalesce_ne/2", "coalesce_ne('', 'x')", r#""x""#),
    eq("coalesce_ne/2", "coalesce_ne(payload.value, 'x')", r#""x""#),
    on(
        r#"{"value": "v"}"#,
        eq("coalesce_ne/2", "coalesce_ne(payload.value, 'x')", r#""v""#),
    ),
    // More than two candidates go in one array: EMQX's source defines `coalesce/1` and
    // `coalesce_ne/1` over a list (`coalesce([undefined | T]) -> coalesce(T);
    // coalesce([H | _T]) -> H.`, `coalesce_ne` also skipping `""`), and the
    // two-argument forms as `coalesce([A, B])`. It has no wider form.
    eq(
        "emqx_rule_funcs.erl coalesce/1",
        "coalesce([payload.x, payload.y, 3])",
        "3",
    ),
    on(
        r#"{"y": "v"}"#,
        eq(
            "emqx_rule_funcs.erl coalesce/1",
            "coalesce([payload.x, payload.y, 3])",
            r#""v""#,
        ),
    ),
    eq(
        "emqx_rule_funcs.erl coalesce_ne/1",
        "coalesce_ne([payload.x, '', 'v'])",
        r#""v""#,
    ),
];

// --- emqx_rule_funcs.erl: the legacy accessors ---------------------------------------
//
// Not in the reference. Each is a fun over the message in EMQX's source; its first
// clause reads one field (`topic() -> fun([_, #{topic := Topic}]) -> Topic`,
// `topic(I)` is `lists:nth(I, emqx_topic:tokens(Topic))`, `flag(Name)` is
// `nested_get({var, Name}, Flags)`, `clientip()` is `peerhost()`, `payload(Path)`
// decodes the payload and reads `map_path(Path)`), and this engine returns that field.

const LEGACY: &[Ex] = &[
    eq(
        "emqx_rule_funcs.erl topic/0",
        "topic()",
        r#""devices/A_C001/data""#,
    ),
    eq("emqx_rule_funcs.erl topic/1", "topic(2)", r#""A_C001""#),
    eq(
        "emqx_rule_funcs.erl clientid/0",
        "clientid()",
        r#""c_emqx""#,
    ),
    eq(
        "emqx_rule_funcs.erl username/0",
        "username()",
        r#""u_emqx""#,
    ),
    eq("emqx_rule_funcs.erl qos/0", "qos()", "1"),
    eq(
        "emqx_rule_funcs.erl flags/0",
        "flags()",
        r#"{"dup":false,"retain":true}"#,
    ),
    eq("emqx_rule_funcs.erl flag/1", "flag('retain')", "true"),
    eq(
        "emqx_rule_funcs.erl peerhost/0",
        "peerhost()",
        r#""127.0.0.1""#,
    ),
    eq(
        "emqx_rule_funcs.erl clientip/0",
        "clientip()",
        r#""127.0.0.1""#,
    ),
    on(
        r#"{"a":{"b":3}}"#,
        eq(
            "emqx_rule_funcs.erl payload/0",
            "payload()",
            r#""{\"a\":{\"b\":3}}""#,
        ),
    ),
    on(
        r#"{"a":{"b":3}}"#,
        eq("emqx_rule_funcs.erl payload/1", "payload('a.b')", "3"),
    ),
    // `msgid()` reads the message's `id`: the same value, in EMQX's 32-hex-digit form.
    eq("emqx_rule_funcs.erl msgid/0", "msgid() = id", "true"),
    shape("emqx_rule_funcs.erl msgid/0", "msgid()", message_id),
];

/// Every table, for [`every_implemented_function_has_an_example`].
const ALL: &[&[Ex]] = &[
    MATH,
    TYPE_JUDGMENT,
    CONVERSION,
    STRINGS,
    MAPS,
    ARRAYS,
    HASHING,
    BITS,
    ENCODING,
    TIME,
    UUID,
    CONDITIONAL,
    LEGACY,
    COMPRESSION,
    BIT_SEQUENCES,
    SYSTEM,
    UNDOCUMENTED,
];

// --- running a row -----------------------------------------------------------------

const TOPIC: &str = "devices/A_C001/data";

/// The expression's value as the exact JSON text the engine renders, or the SQL's error.
fn eval(ex: &Ex) -> Result<String, String> {
    let payload = Bytes::from_static(ex.payload.as_bytes());
    let props = mqtt_core::AppProperties::default();
    let mut msg = PublishInput::new("c_emqx", TOPIC, &payload, 1, &props);
    msg.username = Some("u_emqx");
    msg.peer = Some(SocketAddr::from(([127, 0, 0, 1], 52000)));
    msg.retain = true;
    msg.node = "node-0";
    let outputs = test_sql(&format!("SELECT {} AS r FROM \"#\"", ex.expr), &msg)?;
    let [output] = outputs.as_slice() else {
        panic!(
            "{} `{}`: one output expected, got {outputs:?}",
            ex.doc, ex.expr
        );
    };
    let value = output
        .strip_prefix(r#"{"r":"#)
        .and_then(|v| v.strip_suffix('}'))
        .unwrap_or_else(|| {
            panic!(
                "{} `{}`: output {output} is not {{\"r\":…}}",
                ex.doc, ex.expr
            )
        });
    Ok(value.to_owned())
}

fn now_ns() -> i128 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970");
    i128::try_from(since.as_nanos()).expect("ns since 1970 fit an i128")
}

/// `got` against EMQX's `emqx` within 1e-12 relative, and printed as a float.
fn within_last_digit(emqx: &str, got: &str) -> Result<(), String> {
    let want: f64 = emqx.parse().expect("EMQX's value is a float");
    let value: f64 = got
        .parse()
        .map_err(|_| format!("{got} is not a number (EMQX: {emqx})"))?;
    if !got.contains('.') {
        return Err(format!("{got} is not printed as a float (EMQX: {emqx})"));
    }
    if ((value - want) / want).abs() > 1e-12 {
        return Err(format!("{got} is not within 1e-12 of EMQX's {emqx}"));
    }
    Ok(())
}

/// Run every row of `table`; fail with every row that differs, citing its section.
fn check(table: &[Ex]) {
    let mut wrong = Vec::new();
    for ex in table {
        let from_ns = now_ns();
        let got = eval(ex);
        let window = Window {
            from_ns,
            to_ns: now_ns(),
        };
        let verdict = match (&ex.want, &got) {
            (Want::Json(want), Ok(got)) if got == want => Ok(()),
            (Want::Near(emqx), Ok(got)) => within_last_digit(emqx, got),
            (Want::Fails(want), Err(e)) if e == want => Ok(()),
            (Want::Shape(f), Ok(got)) => f(got, window),
            (want, got) => Err(format!("want {want:?}, got {got:?}")),
        };
        if let Err(why) = verdict {
            wrong.push(format!(
                "  {} `{}` (payload {:?}): {why}",
                ex.doc, ex.expr, ex.payload
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} examples differ from rule-sql-builtin-functions.md:\n{}",
        wrong.len(),
        table.len(),
        wrong.join("\n")
    );
}

// --- shapes of the values that change on every call ---------------------------------

fn json_str(got: &str) -> Result<String, String> {
    match json_decode(got.as_bytes()) {
        Ok(Value::Str(s)) => Ok(s.to_string()),
        other => Err(format!("{got} is not a JSON string ({other:?})")),
    }
}

fn json_int(got: &str) -> Result<i64, String> {
    match json_decode(got.as_bytes()) {
        Ok(Value::Int(n)) => Ok(n),
        other => Err(format!("{got} is not a JSON integer ({other:?})")),
    }
}

fn unit_interval_float(got: &str, _: Window) -> Result<(), String> {
    match json_decode(got.as_bytes()) {
        Ok(Value::Float(f)) if (0.0..1.0).contains(&f) => Ok(()),
        other => Err(format!("{got} is not a float in [0, 1) ({other:?})")),
    }
}

/// `got` is an integer count of `unit_ns` since the epoch, read within the window.
fn now_in(got: &str, w: Window, unit_ns: i128) -> Result<(), String> {
    let n = i128::from(json_int(got)?);
    let (lo, hi) = (w.from_ns.div_euclid(unit_ns), w.to_ns.div_euclid(unit_ns));
    if (lo..=hi).contains(&n) {
        Ok(())
    } else {
        Err(format!("{n} is outside the call's window [{lo}, {hi}]"))
    }
}

fn now_unix_seconds(got: &str, w: Window) -> Result<(), String> {
    now_in(got, w, 1_000_000_000)
}

fn now_unix_seconds_text(got: &str, w: Window) -> Result<(), String> {
    let n: i64 = json_str(got)?
        .parse()
        .map_err(|e| format!("{got} is not a number of seconds: {e}"))?;
    now_in(&n.to_string(), w, 1_000_000_000)
}

fn now_unix_micros(got: &str, w: Window) -> Result<(), String> {
    now_in(got, w, 1_000)
}

/// The reference's RFC 3339 form for an instant in this host's zone, rendered here
/// independently of the engine: `YYYY-MM-DDTHH:MM:SS[.fraction]±hh:mm` with `digits`
/// fractional digits (0, 3 or 6).
fn local_rfc3339(secs: i64, nanos: u32, digits: u32) -> String {
    let utc = chrono::DateTime::from_timestamp(secs, nanos).expect("a representable instant");
    let offset = chrono::Local
        .offset_from_utc_datetime(&utc.naive_utc())
        .local_minus_utc();
    let local = utc.naive_utc() + chrono::Duration::seconds(i64::from(offset));
    let fraction = match digits {
        0 => String::new(),
        3 => format!(".{:03}", nanos / 1_000_000),
        6 => format!(".{:06}", nanos / 1_000),
        _ => unreachable!("the reference shows seconds, milliseconds and microseconds"),
    };
    let sign = if offset < 0 { '-' } else { '+' };
    let abs = offset.unsigned_abs();
    format!(
        "{}{fraction}{sign}{:02}:{:02}",
        local.format("%Y-%m-%dT%H:%M:%S"),
        abs / 3600,
        abs % 3600 / 60
    )
}

/// `got` is an RFC 3339 time read within the window, in the reference's form with
/// `digits` fractional digits, in this host's zone.
fn now_rfc3339_in(got: &str, w: Window, digits: u32) -> Result<(), String> {
    let s = json_str(got)?;
    let t = chrono::DateTime::parse_from_rfc3339(&s).map_err(|e| format!("{s}: {e}"))?;
    let expected = local_rfc3339(t.timestamp(), t.timestamp_subsec_nanos(), digits);
    if s != expected {
        return Err(format!("{s} is not in the reference's form {expected}"));
    }
    let unit_ns = 10_i128.pow(9 - digits);
    let n = i128::from(t.timestamp()) * 1_000_000_000 + i128::from(t.timestamp_subsec_nanos());
    let (lo, hi) = (w.from_ns.div_euclid(unit_ns), w.to_ns.div_euclid(unit_ns));
    if (lo..=hi).contains(&n.div_euclid(unit_ns)) {
        Ok(())
    } else {
        Err(format!("{s} is outside the call's window"))
    }
}

fn now_rfc3339_seconds(got: &str, w: Window) -> Result<(), String> {
    now_rfc3339_in(got, w, 0)
}

fn now_rfc3339_micros(got: &str, w: Window) -> Result<(), String> {
    now_rfc3339_in(got, w, 6)
}

fn local_offset_seconds(got: &str, _: Window) -> Result<(), String> {
    let want = i64::from(chrono::Local::now().offset().local_minus_utc());
    match json_int(got)? {
        n if n == want => Ok(()),
        n => Err(format!("{n} is not this host's offset {want}")),
    }
}

/// `got` renders the reference's instant (`documented`, as the reference prints it in
/// its writer's +08:00 zone) in this host's zone, in the reference's form.
fn renders_instant(got: &str, documented: &str, digits: u32) -> Result<(), String> {
    let s = json_str(got)?;
    let t = chrono::DateTime::parse_from_rfc3339(documented).expect("the reference's time");
    let expected = local_rfc3339(t.timestamp(), t.timestamp_subsec_nanos(), digits);
    if s == expected {
        Ok(())
    } else {
        Err(format!(
            "{s} is not {documented} in this host's zone, {expected}"
        ))
    }
}

fn documented_instant_seconds(got: &str, _: Window) -> Result<(), String> {
    renders_instant(got, "2024-02-23T15:00:00+08:00", 0)
}

fn documented_instant_millis(got: &str, _: Window) -> Result<(), String> {
    renders_instant(got, "2024-02-23T15:00:00.766+08:00", 3)
}

/// 32 lower-case hex digits with version 4 (digit 13) and the RFC 4122 variant (digit
/// 17 in `8..=b`).
fn uuid_v4_hex(h: &str) -> Result<(), String> {
    let lower_hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
    if h.len() != 32 || !h.chars().all(lower_hex) {
        return Err(format!("{h} is not 32 lower-case hex digits"));
    }
    if &h[12..13] != "4" || !"89ab".contains(&h[16..17]) {
        return Err(format!("{h} is not a version 4, RFC 4122 variant UUID"));
    }
    Ok(())
}

fn uuid_v4_hyphenated(got: &str, _: Window) -> Result<(), String> {
    let s = json_str(got)?;
    let groups: Vec<usize> = s.split('-').map(str::len).collect();
    if groups != [8, 4, 4, 4, 12] {
        return Err(format!("{s} is not grouped 8-4-4-4-12"));
    }
    uuid_v4_hex(&s.replace('-', ""))
}

fn uuid_v4_bare(got: &str, _: Window) -> Result<(), String> {
    uuid_v4_hex(&json_str(got)?)
}

fn message_id(got: &str, _: Window) -> Result<(), String> {
    let s = json_str(got)?;
    let upper_hex = |c: char| c.is_ascii_digit() || ('A'..='F').contains(&c);
    if s.len() == 32 && s.chars().all(upper_hex) {
        Ok(())
    } else {
        Err(format!("{s} is not 32 upper-case hex digits"))
    }
}

// --- the tests -------------------------------------------------------------------------

/// docs/RULES.md (Functions): "Every function below is named, typed and behaves as in
/// EMQX's built-in function reference." Each of the reference's math examples gives EMQX's value, as an
/// integer where EMQX's is (`ceil`, `floor`, `round`) and a float where it is
/// (`power(2, 3) = 8.0`); a function that rounded differently or returned the other
/// number type would fail its row.
#[test]
fn math_functions_match_emqx_examples() {
    check(MATH);
}

/// docs/RULES.md (Functions, "Type checks"): every type-judgment example in EMQX's
/// reference, including the null pair EMQX distinguishes (`undefined` versus JSON
/// `null`). Treating `null` as `undefined`, or a numeric string as a number, fails a
/// row.
#[test]
fn type_judgment_functions_match_emqx_examples() {
    check(TYPE_JUDGMENT);
}

/// docs/RULES.md (Functions, "Conversion"): EMQX's conversion examples, including the
/// float formatting rules (`str` keeps at most 10 decimals, `float2str` prints the
/// binary approximation) and the `# Wrong` inputs that must fail the rule rather than
/// convert to something.
#[test]
fn conversion_functions_match_emqx_examples() {
    check(CONVERSION);
}

/// docs/RULES.md (Functions, "Strings"): EMQX's string examples, including the
/// reference's `unescape` walk-through on a newline-separated payload. A change to
/// trimming, padding direction, split options or regex extraction fails a row.
#[test]
fn string_functions_match_emqx_examples() {
    check(STRINGS);
}

/// docs/RULES.md (Functions, "Maps"): EMQX's map examples. A missing key reads as
/// `undefined` (rendered `"undefined"`, as EMQX's SQL test shows it), and nested key
/// lists read and build nested maps.
#[test]
fn map_functions_match_emqx_examples() {
    check(MAPS);
}

/// docs/RULES.md (Functions, "Arrays"): EMQX's array examples, with positions counted
/// from 1 and the documented out-of-range calls failing the rule.
#[test]
fn array_functions_match_emqx_examples() {
    check(ARRAYS);
}

/// docs/RULES.md (Functions, "Hashing"): EMQX's digests and its two bucketing
/// functions. `hash_to_range` and `map_to_range` exist to give the same bucket EMQX
/// gives, so any change to the mapping fails a row.
#[test]
fn hashing_functions_match_emqx_examples() {
    check(HASHING);
}

/// docs/RULES.md (Functions, "Bits"): EMQX's bit examples, including the negative
/// inputs whose sign-extending shifts and two's-complement results the reference
/// shows.
#[test]
fn bit_functions_match_emqx_examples() {
    check(BITS);
}

/// docs/RULES.md (Functions, "Encoding"): EMQX's encoding examples, and the
/// `'urlsafe'` / `'no_padding'` options RULES.md says both base64 functions take.
#[test]
fn encoding_functions_match_emqx_examples() {
    check(ENCODING);
}

/// docs/RULES.md (Functions, "Time"): EMQX's time examples. Parsing and formatting
/// with an explicit offset are exact; the clock and local-zone functions give a value
/// in the reference's exact form, read during the call, in this host's zone.
#[test]
fn time_functions_match_emqx_examples() {
    check(TIME);
}

/// docs/RULES.md (Functions, "UUID"): version 4 UUIDs in the reference's two forms.
#[test]
fn uuid_functions_match_emqx_examples() {
    check(UUID);
}

/// docs/RULES.md (Functions, "Conditional"): the reference's prose examples for
/// `coalesce` and `coalesce_ne`, the `CASE` expression it calls equivalent, and EMQX's
/// one-argument forms over a list of candidates. Returning a missing value, or skipping
/// a present one, fails a row.
#[test]
fn conditional_functions_match_emqx_examples() {
    check(CONDITIONAL);
}

/// docs/RULES.md (Functions, "Legacy accessors"): each accessor returns the message
/// field its EMQX definition reads, including `topic(n)` counting levels from 1 and
/// `payload('a.b')` reading a path.
#[test]
fn legacy_accessors_match_emqx_source() {
    check(LEGACY);
}

/// docs/RULES.md (Functions, "Compression"): the reference's compression examples,
/// byte for byte — the backends are the C zlib and liblz4 EMQX links.
#[test]
fn compression_functions_match_emqx_examples() {
    check(COMPRESSION);
}

/// docs/RULES.md (Functions, "Bit sequences"): the reference's `bitsize`, `bytesize`
/// and `subbits` examples, including its half-precision float.
#[test]
fn bit_sequence_functions_match_emqx_examples() {
    check(BIT_SEQUENCES);
}

/// docs/RULES.md (Functions, "System"): `getenv` reads `EMQXVAR_<name>`.
#[test]
fn system_functions_match_emqx_examples() {
    check(SYSTEM);
}

/// docs/RULES.md (Functions, "Callable in EMQX, undocumented there"): every export of
/// EMQX's `emqx_rule_funcs` is a SQL function there; the ones its reference leaves out
/// behave as its source does.
#[test]
fn undocumented_emqx_functions_match_emqx_source() {
    check(UNDOCUMENTED);
}

/// docs/RULES.md (Functions): "That reference's examples run as this engine's unit
/// tests". Every function
/// the engine implements is called in some row above, so a function added without an
/// example from EMQX's reference (or its source) fails here, naming it.
#[test]
fn every_implemented_function_has_an_example() {
    let mut called = std::collections::BTreeSet::new();
    for ex in ALL.iter().flat_map(|t| t.iter()) {
        let mut in_literal = false;
        let mut ident = String::new();
        for c in ex.expr.chars() {
            if c == '\'' {
                in_literal = !in_literal;
            }
            if !in_literal && (c.is_ascii_alphanumeric() || c == '_') {
                ident.push(c);
                continue;
            }
            if c == '(' && !ident.is_empty() {
                called.insert(std::mem::take(&mut ident));
            }
            ident.clear();
        }
    }
    let missing: Vec<&str> = function_names()
        .into_iter()
        .filter(|f| !called.contains(*f))
        .collect();
    assert!(
        missing.is_empty(),
        "implemented functions with no example row: {missing:?}"
    );
}
