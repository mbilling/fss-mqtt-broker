//! The engine against EMQX's documented behaviour. Where a test cites "EMQX docs",
//! the statement, input and expected output are taken from EMQX's rule SQL reference
//! (`rule-sql-syntax.md`, `rule-sql-builtin-functions.md`,
//! `rule-sql-events-and-fields.md`); those examples are the compatibility oracle.

use super::*;

/// A message on `topic` from client `c_emqx` / user `u_emqx`.
fn msg<'a>(
    topic: &'a str,
    payload: &'a Bytes,
    props: &'a mqtt_core::AppProperties,
) -> PublishInput<'a> {
    let mut m = PublishInput::new("c_emqx", topic, payload, 1, props);
    m.username = Some("u_emqx");
    m.peer = Some("127.0.0.1:52000".parse().unwrap());
    m.node = "node-0";
    m
}

/// Outputs of `sql` for a message with `payload` on `topic`.
fn run_on(sql: &str, topic: &str, payload: &str) -> Result<Vec<String>, String> {
    let payload = Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    test_sql(sql, &msg(topic, &payload, &props))
}

fn one(sql: &str, payload: &str) -> String {
    let out = run_on(sql, "t/a", payload).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(out.len(), 1, "{sql}: {out:?}");
    out.into_iter().next().unwrap()
}

/// The value of `expr` (selected `AS r`) for a `{}` payload.
fn val(expr: &str) -> String {
    let out = one(&format!("SELECT {expr} AS r FROM \"t/#\""), "{}");
    let v = json_decode(out.as_bytes()).unwrap();
    let Value::Map(m) = v else { panic!() };
    m.get("r").unwrap().to_json().unwrap()
}

fn fails(sql: &str, payload: &str) -> String {
    run_on(sql, "t/a", payload).expect_err(sql)
}

#[test]
fn select_fields_and_aliases_emqx_docs() {
    assert_eq!(
        one(
            "SELECT payload.msg as msg, clientid, username, payload, topic, qos FROM \"t/#\"",
            r#"{"msg":"hello"}"#
        ),
        r#"{"msg":"hello","clientid":"c_emqx","username":"u_emqx","payload":"{\"msg\":\"hello\"}","topic":"t/a","qos":1}"#
    );
}

#[test]
fn unaliased_payload_path_nests_under_payload() {
    assert_eq!(
        one("SELECT payload.a.b FROM \"t/#\"", r#"{"a":{"b":3}}"#),
        r#"{"payload":{"a":{"b":3}}}"#
    );
}

#[test]
fn where_filters_and_can_use_select_aliases() {
    let sql = "SELECT payload.x as y FROM \"t/#\" WHERE y = 1";
    assert_eq!(run_on(sql, "t/1", r#"{"x":1}"#).unwrap().len(), 1);
    assert!(run_on(sql, "t/1", r#"{"x":2}"#).unwrap().is_empty());
    let nested = "SELECT * FROM \"#\" WHERE payload.x.y = 1";
    assert_eq!(
        run_on(nested, "a", r#"{"x":{"y":1},"other":"field"}"#)
            .unwrap()
            .len(),
        1
    );
}

/// EMQX's `evaluate_select`: the WHERE reads `maps:merge(Columns, Selected)`, one map. A
/// selected top-level key hides the input's whole key; nothing falls through into it.
#[test]
fn the_where_reads_the_input_with_the_selection_merged_over_it() {
    // An unaliased `payload.x` selects `payload` as `{"x": 1}`, which hides the input's
    // `payload` from the WHERE: `payload.y` is undefined there.
    let sql = r#"SELECT payload.x FROM "t/#" WHERE payload.y = 1"#;
    assert!(run_on(sql, "t/a", r#"{"x":1,"y":1}"#).unwrap().is_empty());
    // An alias that shadows an input name hides it, even when the value is undefined.
    let sql = r#"SELECT payload.missing AS clientid FROM "t/#" WHERE clientid = 'c_emqx'"#;
    assert!(run_on(sql, "t/a", "{}").unwrap().is_empty());
    // A dotted alias replaces the whole top-level map, not one key in it.
    let sql = r#"SELECT 1 AS flags.custom FROM "t/#" WHERE flags.retain = false"#;
    assert!(run_on(sql, "t/a", "{}").unwrap().is_empty());
    // A `*` after an alias overwrites it, so the WHERE sees the input's value.
    let sql = r#"SELECT 'shadow' AS clientid, * FROM "t/#" WHERE clientid = 'c_emqx'"#;
    assert_eq!(run_on(sql, "t/a", "{}").unwrap().len(), 1);
    // A name the selection does not write still reads the input.
    let sql = r#"SELECT payload.x AS x FROM "t/#" WHERE topic = 't/a' AND x = 1"#;
    assert_eq!(run_on(sql, "t/a", r#"{"x":1}"#).unwrap().len(), 1);
}

/// EMQX evaluates every SELECT field before the WHERE, so a field that fails fails the
/// rule even for a message the WHERE would turn away.
#[test]
fn a_failing_select_field_fails_the_rule_even_when_the_where_is_false() {
    fails(
        r#"SELECT int(payload.x) AS y FROM "t/#" WHERE 1 = 2"#,
        r#"{"x":"abc"}"#,
    );
    fails(
        r##"SELECT payload.x AS x FROM "#" WHERE topic = 'other'"##,
        "not json",
    );
}

/// INCASE runs before DO for each element, against the input, the selection and the
/// element merged into one map: it cannot see DO's aliases, and a selected key hides the
/// input's.
#[test]
fn incase_and_do_read_the_merged_scope_like_emqx() {
    let p = r#"{"s":[{"t":20},{"t":40}],"y":7,"a":{"z":1}}"#;
    // DO's alias `t` is not visible to INCASE.
    let out = run_on(
        r#"FOREACH payload.s AS s DO s.t AS t INCASE t > 30 FROM "t/#""#,
        "t/a",
        p,
    );
    assert!(out.unwrap().is_empty());
    let out = run_on(
        r#"FOREACH payload.s AS s DO s.t AS t INCASE s.t > 30 FROM "t/#""#,
        "t/a",
        p,
    );
    assert_eq!(out.unwrap(), [r#"{"t":40}"#]);
    // A leading FOREACH field selected as `payload` hides the input's payload from DO,
    // which reads `[DoSelected, merge(Columns, Selected, Item)]`.
    let out = run_on(
        r#"FOREACH payload.a AS payload, payload.s AS s DO payload.y AS py, payload.z AS pz FROM "t/#""#,
        "t/a",
        p,
    );
    // `py` is undefined, not the input's `"y": 7`: the selected `payload` hides it.
    let row = r#"{"py":"undefined","pz":1}"#;
    assert_eq!(out.unwrap(), [row, row]);
}

#[test]
fn expressions_emqx_docs() {
    assert_eq!(
        one(
            "SELECT (upper(clientid) + '_UPPERCASE_LETTERS') as cid FROM \"t/#\"",
            "{}"
        ),
        r#"{"cid":"C_EMQX_UPPERCASE_LETTERS"}"#
    );
    assert_eq!(
        one(
            "SELECT (payload.integer_field + 2) * 2 as num FROM \"t/#\"",
            r#"{"integer_field":3}"#
        ),
        r#"{"num":10}"#
    );
    assert_eq!(
        one(
            "SELECT payload.a.b.c.deep as my_field FROM \"t/#\"",
            r#"{"a":{"b":{"c":{"deep":"x"}}}}"#
        ),
        r#"{"my_field":"x"}"#
    );
}

#[test]
fn case_expression_emqx_docs() {
    let sql = "SELECT CASE WHEN payload.x < 0 THEN 0 WHEN payload.x > 7 THEN 7 ELSE payload.x END as x FROM \"t/#\"";
    assert_eq!(one(sql, r#"{"x":8}"#), r#"{"x":7}"#);
    assert_eq!(one(sql, r#"{"x":-1}"#), r#"{"x":0}"#);
    assert_eq!(one(sql, r#"{"x":3}"#), r#"{"x":3}"#);
    // The switch form is an exact match.
    assert_eq!(
        one(
            "SELECT CASE payload.k WHEN 'a' THEN 1 WHEN 'b' THEN 2 END as n FROM \"t/#\"",
            r#"{"k":"b"}"#
        ),
        r#"{"n":2}"#
    );
}

#[test]
fn foreach_with_do_and_incase_emqx_docs() {
    let sql = "FOREACH payload.sensors as sensor DO clientid, upper(sensor.name) as name, sensor.idx as idx INCASE sensor.idx >= 1 FROM \"t/#\"";
    let out = run_on(
        sql,
        "t/1",
        r#"{"sensors":[{"idx":0,"name":"t0"},{"idx":1,"name":"t1"},{"idx":2,"name":"t2"}]}"#,
    )
    .unwrap();
    assert_eq!(
        out,
        [
            r#"{"clientid":"c_emqx","name":"T1","idx":1}"#,
            r#"{"clientid":"c_emqx","name":"T2","idx":2}"#
        ]
    );
}

#[test]
fn foreach_default_item_and_chained_aliases_emqx_docs() {
    let payload = Bytes::from_static(
        br#"{"date":"2020-04-24","data":{"sensors":[{"name":"a","idx":0},{"name":"b","idx":1}]}}"#,
    );
    let props = mqtt_core::AppProperties::default();
    let mut m = msg("t/1", &payload, &props);
    m.clientid = "c_steve";
    let out = test_sql(
        "FOREACH payload.data as d, d.sensors as s DO nth(2, tokens(clientid,'_')) as clientid, s.name as name, s.idx as idx INCASE s.idx >= 1 FROM \"t/#\"",
        &m,
    )
    .unwrap();
    assert_eq!(out, [r#"{"clientid":"steve","name":"b","idx":1}"#]);
    // Without `as`, the element is `item`.
    let out = test_sql(
        "FOREACH payload.data.sensors DO item.name as n FROM \"t/#\"",
        &m,
    )
    .unwrap();
    assert_eq!(out, [r#"{"n":"a"}"#, r#"{"n":"b"}"#]);
}

#[test]
fn foreach_over_a_non_array_yields_nothing_and_is_capped() {
    assert!(run_on("FOREACH payload.x FROM \"t/#\"", "t", r#"{"x":5}"#)
        .unwrap()
        .is_empty());
    let big: Vec<String> = (0..300).map(|i| i.to_string()).collect();
    let payload = format!("{{\"a\":[{}]}}", big.join(","));
    let err = fails("FOREACH payload.a DO item as v FROM \"t/#\"", &payload);
    assert!(err.contains("more than 256"), "{err}");
}

#[test]
fn user_properties_and_pub_props_emqx_docs() {
    let payload = Bytes::from_static(b"x");
    let props = mqtt_core::AppProperties {
        user_properties: vec![("foo".into(), "bar".into()), ("foo".into(), "baz".into())],
        content_type: Some("text/plain".into()),
        ..Default::default()
    };
    let m = msg("t/1", &payload, &props);
    let out = test_sql(
        "SELECT pub_props.'User-Property'.foo as foo, pub_props.'Content-Type' as ct, pub_props.'User-Property-Pairs' as pairs FROM \"t/#\"",
        &m,
    )
    .unwrap();
    assert_eq!(
        out,
        [
            r#"{"foo":"baz","ct":"text/plain","pairs":[{"key":"foo","value":"bar"},{"key":"foo","value":"baz"}]}"#
        ]
    );
}

#[test]
fn topic_match_operator_and_topic_function() {
    let sql = "SELECT topic(2) as dev FROM \"#\" WHERE topic =~ 'sensors/+/data'";
    assert_eq!(
        run_on(sql, "sensors/d1/data", "{}").unwrap(),
        [r#"{"dev":"d1"}"#]
    );
    assert!(run_on(sql, "sensors/d1/cfg", "{}").unwrap().is_empty());
}

#[test]
fn comparison_semantics_follow_emqx() {
    // undefined compares false with a value, equal with undefined.
    assert!(run_on(
        "SELECT 1 as a FROM \"t/#\" WHERE payload.nope = 1",
        "t",
        "{}"
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        run_on(
            "SELECT 1 as a FROM \"t/#\" WHERE nope = also_nope",
            "t",
            "{}"
        )
        .unwrap()
        .len(),
        1
    );
    // A numeric string compares as a number.
    assert_eq!(
        run_on(
            "SELECT 1 as a FROM \"t/#\" WHERE payload.s > 10",
            "t",
            r#"{"s":"20"}"#
        )
        .unwrap()
        .len(),
        1
    );
    // ... and a non-numeric one fails the rule.
    assert!(fails(
        "SELECT 1 as a FROM \"t/#\" WHERE payload.s > 10",
        r#"{"s":"abc"}"#
    )
    .contains("cannot compare"));
    // A boolean against its text.
    assert_eq!(
        run_on(
            "SELECT 1 as a FROM \"t/#\" WHERE payload.b = 'true'",
            "t",
            r#"{"b":true}"#
        )
        .unwrap()
        .len(),
        1
    );
    // 1 == 1.0, but IN is exact.
    assert_eq!(val("1 = 1.0"), "true");
    assert_eq!(val("1 IN (1.0, 2)"), "false");
    assert_eq!(val("'b' IN ('a', 'b')"), "true");
    assert_eq!(val("'z' NOT IN ('a', 'b')"), "true");
    assert_eq!(val("NOT 5"), "false");
}

#[test]
fn a_non_json_payload_fails_only_rules_that_reach_into_it() {
    assert!(fails("SELECT payload.x FROM \"t/#\"", "not json").contains("not JSON"));
    assert_eq!(
        one("SELECT payload FROM \"t/#\"", "not json"),
        r#"{"payload":"not json"}"#
    );
}

#[test]
fn arithmetic_follows_erlang() {
    assert_eq!(val("7 / 2"), "3.5");
    assert_eq!(val("7 div 2"), "3");
    assert_eq!(val("-7 div 2"), "-3");
    assert_eq!(val("-7 mod 2"), "-1");
    assert_eq!(val("'a' + 1"), r#""a1""#);
    assert!(fails("SELECT 1 / 0 as r FROM \"t/#\"", "{}").contains("division by zero"));
    // Erlang integers have no width: past 64 bits arithmetic goes on.
    assert_eq!(val("9223372036854775807 + 1"), "9223372036854775808");
    assert!(fails("SELECT 1 - 'a' as r FROM \"t/#\"", "{}").contains("arithmetic"));
}

#[test]
fn indices_and_ranges() {
    let p = r#"{"a":[10,20,30,40]}"#;
    assert_eq!(
        one(
            "SELECT payload.a[1] as f, payload.a[-1] as l, payload.a[9] as none FROM \"t/#\"",
            p
        ),
        r#"{"f":10,"l":40,"none":"undefined"}"#
    );
    assert_eq!(
        one("SELECT payload.a[2..3] as s FROM \"t/#\"", p),
        r#"{"s":[20,30]}"#
    );
    assert_eq!(
        one("SELECT payload.a[2..-1] as s FROM \"t/#\"", p),
        r#"{"s":[20,30,40]}"#
    );
    assert_eq!(val("[1..3]"), "[1,2,3]");
    assert_eq!(val("['x', 1 + 1]"), r#"["x",2]"#);
}

#[test]
fn conversion_functions_emqx_docs() {
    assert_eq!(val("bool(0)"), "false");
    assert_eq!(val("bool('false')"), "false");
    assert_eq!(val("float(20)"), "20.0");
    // jiffy writes the float in Erlang's shortest form.
    assert_eq!(val("float('3.14e4')"), "3.14e4");
    assert_eq!(val("float('3.1415926', 3)"), "3.142");
    assert_eq!(val("float2str(0.1, 5)"), r#""0.1""#);
    assert_eq!(val("float2str(0.100001, 5)"), r#""0.1""#);
    assert_eq!(val("int(true)"), "1");
    assert_eq!(val("int(3.14)"), "3");
    assert_eq!(val("int(-3.14)"), "-4");
    assert_eq!(val("int('-100')"), "-100");
    assert_eq!(val("int('+200')"), "200");
    assert_eq!(val("int('0010')"), "10");
    assert_eq!(val("int('3.1415e2')"), "314");
    assert_eq!(val("int(substr('Number 100', 7))"), "100");
    assert_eq!(val("str(100)"), r#""100""#);
    assert_eq!(val("str(0.30000000040)"), r#""0.3000000004""#);
    assert_eq!(val("str(3.14159265359)"), r#""3.1415926536""#);
    assert_eq!(
        val("str(json_decode('[{\"msg\": \"hello\"}]'))"),
        r#""[{\"msg\":\"hello\"}]""#
    );
    assert!(fails("SELECT bool(20) as r FROM \"t/#\"", "{}").contains("bool()"));
    assert!(fails("SELECT int('Number 100') as r FROM \"t/#\"", "{}").contains("int()"));
}

#[test]
fn type_judgment_functions_emqx_docs() {
    assert_eq!(val("is_array([1, 2])"), "true");
    assert_eq!(val("is_array('[1, 2]')"), "false");
    assert_eq!(val("is_map(json_decode('{\"value\": 1}'))"), "true");
    assert_eq!(val("is_null(this_is_an_unassigned_variable)"), "true");
    assert_eq!(
        val("is_null(map_get('b', json_decode('{\"b\": null}')))"),
        "false"
    );
    assert_eq!(
        val("is_null_var(map_get('b', json_decode('{\"b\": null}')))"),
        "true"
    );
    assert_eq!(val("is_num('123')"), "false");
    assert_eq!(val("is_float(123)"), "false");
    assert_eq!(val("is_empty('{}')"), "true");
    assert_eq!(val("is_empty(map_get('key', '{\"key\" : []}'))"), "true");
}

#[test]
fn string_functions_emqx_docs() {
    assert_eq!(val("ascii('abc')"), "97");
    assert_eq!(val("concat('Name:', 'John')"), r#""Name:John""#);
    assert_eq!(val("find('..., Value: 1.2', 'Value:')"), r#""Value: 1.2""#);
    assert_eq!(val("find('..., Value: 1.2', 'Data')"), r#""""#);
    assert_eq!(
        val("find('Front, Middle, End', ', ', 'trailing')"),
        r#"", End""#
    );
    assert_eq!(val("join_to_string(', ', ['a', 'b', 'c'])"), r#""a, b, c""#);
    assert_eq!(val("pad('hello', 8, 'both')"), r#"" hello  ""#);
    assert_eq!(
        val("pad('hello', 8, 'trailing', 'abc')"),
        r#""helloabcabcabc""#
    );
    assert_eq!(val("regex_match('123', '^\\d+$')"), "true");
    assert_eq!(val("regex_replace('a;b; c', ';\\s*', ',')"), r#""a,b,c""#);
    assert_eq!(
        val("regex_extract('Date: 2021-05-20', '(\\d{4})-(\\d{2})-(\\d{2})')"),
        r#"["2021","05","20"]"#
    );
    assert_eq!(val("regex_extract('No numbers here!', '(\\d+)')"), "[]");
    assert_eq!(
        val("replace('ab..cd..ef', '..', '**', 'trailing')"),
        r#""ab..cd**ef""#
    );
    assert_eq!(val("replace('ab..cd..ef', '..', '')"), r#""abcdef""#);
    assert_eq!(val("reverse('hello')"), r#""olleh""#);
    assert_eq!(val("rm_prefix('foo/bar', 'foo/')"), r#""bar""#);
    assert_eq!(val("split('a;;b;;c', ';')"), r#"["a","b","c"]"#);
    assert_eq!(
        val("split('a;;b;;c', ';', 'notrim')"),
        r#"["a","","b","","c"]"#
    );
    assert_eq!(val("split('a;b;c', ';', 'leading')"), r#"["a","b;c"]"#);
    assert_eq!(val("split('a;b;c', ';', 'trailing')"), r#"["a;b","c"]"#);
    assert_eq!(
        val("split(';a;b;c', ';', 'leading_notrim')"),
        r#"["","a;b;c"]"#
    );
    assert_eq!(val("sprintf('hello, ~s!', 'steve')"), r#""hello, steve!""#);
    assert_eq!(val("strlen('hello')"), "5");
    assert_eq!(val("substr('hello world!', 6, 5)"), r#""world""#);
    assert_eq!(val("tokens('a,b;c,d', ',;')"), r#"["a","b","c","d"]"#);
    assert_eq!(val("trim(unescape('\\t  hello \\r\\n'))"), r#""hello""#);
    assert_eq!(
        val("split(unescape('a\\nb'), unescape('\\n'))"),
        r#"["a","b"]"#
    );
    assert_eq!(val("lower('Hello')"), r#""hello""#);
}

#[test]
fn map_and_array_functions_emqx_docs() {
    assert_eq!(
        val("map_get('msg', json_decode('{\"msg\": \"hello\"}'))"),
        r#""hello""#
    );
    assert_eq!(
        val("map_get('data', json_decode('{\"msg\": \"hello\"}'), '')"),
        r#""""#
    );
    assert_eq!(
        val("map_get('b', map_put('b', 1, json_decode('{\"a\": 1}')))"),
        "1"
    );
    assert_eq!(
        val("mget(['a', 'b'], json_decode('{\"a\": {\"b\": 1}}'))"),
        "1"
    );
    assert_eq!(
        val("mget(['a', 'b'], mput(['a', 'b'], 2, json_decode('{\"c\": 1}')))"),
        "2"
    );
    assert_eq!(
        val("map_keys(json_decode('{\"a\": 1, \"b\": 2}'))"),
        r#"["a","b"]"#
    );
    assert_eq!(
        val("map_to_entries(json_decode('{\"a\": 1}'))"),
        r#"[{"key":"a","value":1}]"#
    );
    assert_eq!(val("map_size(json_decode('{}'))"), "0");
    assert_eq!(val("contains(2, [1, 2, 3])"), "true");
    assert_eq!(val("contains(2.3, [1.8, 2.5, 2.0])"), "false");
    assert_eq!(
        val("contains(json_decode('{\"a\": 1}'), [json_decode('{\"a\": 1}')])"),
        "true"
    );
    assert_eq!(val("first(['John', 'David'])"), r#""John""#);
    assert_eq!(val("last(['John', 'David'])"), r#""David""#);
    assert_eq!(val("length([1,2,3,4])"), "4");
    assert_eq!(val("nth(1, [1,2,3])"), "1");
    assert_eq!(val("sublist(3, [1,2,3,4])"), "[1,2,3]");
    assert_eq!(val("sublist(2, 10, [1,2,3,4])"), "[2,3,4]");
    assert!(fails("SELECT nth(4, [1,2,3]) as r FROM \"t/#\"", "{}").contains("nth()"));
    assert!(fails("SELECT first([]) as r FROM \"t/#\"", "{}").contains("first()"));
}

#[test]
fn hashing_encoding_and_bits_emqx_docs() {
    assert_eq!(val("md5('hello')"), r#""5d41402abc4b2a76b9719d911017c592""#);
    assert_eq!(
        val("sha('hello')"),
        r#""aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d""#
    );
    assert_eq!(
        val("sha256('hello')"),
        r#""2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824""#
    );
    assert_eq!(val("hash_to_range('A_C001', 0, 3)"), "0");
    assert_eq!(val("map_to_range(7, 0, 3)"), "3");
    assert_eq!(val("map_to_range('a', 0, 3)"), "1");
    assert_eq!(val("base64_encode('hello')"), r#""aGVsbG8=""#);
    assert_eq!(val("base64_decode('aGVsbG8=')"), r#""hello""#);
    assert_eq!(val("bin2hexstr(base64_decode('y0jN'))"), r#""CB48CD""#);
    assert_eq!(val("base64_encode(hexstr2bin('CB48CD'))"), r#""y0jN""#);
    assert_eq!(
        val("base64_encode('hello', 'no_padding', 'urlsafe')"),
        r#""aGVsbG8""#
    );
    assert_eq!(val("json_encode([1,2,3])"), r#""[1,2,3]""#);
    assert_eq!(val("sqlserver_bin2hexstr('hello')"), r#""0x68656C6C6F""#);
    assert_eq!(val("bitand(-10, -8)"), "-16");
    assert_eq!(val("bitnot(10)"), "-11");
    assert_eq!(val("bitsl(-8, 2)"), "-32");
    assert_eq!(val("bitsr(-8, 6)"), "-1");
    assert_eq!(val("bitor(-10, -8)"), "-2");
    assert_eq!(val("bitxor(-10, -8)"), "14");
    // Bytes that are not text are U+FFFD in JSON, as in EMQX (jiffy's `force_utf8`).
    assert_eq!(
        one("SELECT base64_decode('y0jN') as r FROM \"t/#\"", "{}"),
        "{\"r\":\"\u{FFFD}H\u{FFFD}\"}"
    );
}

#[test]
fn time_functions_emqx_docs() {
    assert_eq!(
        val("format_date('millisecond', '+08:00', '%Y-%m-%d %H:%M:%S.%6N%z', 1708933353472)"),
        r#""2024-02-26 15:42:33.472000+0800""#
    );
    assert_eq!(
        val("format_date('millisecond', '+08:20:30', '%Y-%m-%d %H:%M:%S.%3N%::z', 1708933353472)"),
        r#""2024-02-26 16:03:03.472+08:20:30""#
    );
    assert_eq!(
        val("date_to_unix_ts('second', '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 15:00:00+08:00')"),
        "1708671600"
    );
    assert_eq!(
        val("date_to_unix_ts('second', '+08:00', '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 15:00:00')"),
        "1708671600"
    );
    assert_eq!(
        val("date_to_unix_ts('second', 14400, '%Y-%m-%d %H:%M:%S%:z', '2024-02-23 15:00:00')"),
        "1708686000"
    );
    assert_eq!(
        val("rfc3339_to_unix_ts('2024-02-23T15:56:30Z')"),
        "1708703790"
    );
    assert_eq!(
        val("rfc3339_to_unix_ts('2024-02-23T15:56:30+08:00')"),
        "1708674990"
    );
    assert_eq!(
        val("rfc3339_to_unix_ts('2024-02-23T15:56:30.87Z', 'millisecond')"),
        "1708703790870"
    );
    assert_eq!(val("timezone_to_offset_seconds('+08:00')"), "28800");
    assert_eq!(val("is_int(now_timestamp('millisecond'))"), "true");
    assert_eq!(val("strlen(uuid_v4())"), "36");
    assert_eq!(val("strlen(uuid_v4_no_hyphen())"), "32");
}

#[test]
fn conditional_functions() {
    assert_eq!(val("coalesce(payload.nope, 0)"), "0");
    assert_eq!(val("coalesce_ne('', 'x')"), r#""x""#);
    assert_eq!(val("coalesce(nope, also_nope)"), r#""undefined""#);
}

/// EMQX 6.3.1, `emqx_rule_sqltester:test/1` on each statement and payload, and
/// `emqx_rule_funcs.erl`: `coalesce([]) -> null(); coalesce([undefined | T]) ->
/// coalesce(T); coalesce([H | _T]) -> H.`, `coalesce(A, B) -> coalesce([A, B]).`, with
/// `null() -> undefined`. There is no clause for one argument that is not a list
/// (`bad_sql_function_argument`) and no `coalesce/0` or `/3`
/// (`sql_function_not_supported`). `coalesce_ne` also skips `""` and `<<>>`, and `""`
/// is the empty list. A JSON `null` is a value to both.
#[test]
fn coalesce_takes_a_list_or_two_candidates_as_in_emqx() {
    let a = |expr: &str, payload: &str| one(&format!("SELECT {expr} AS a FROM \"t/#\""), payload);
    // One argument: the list of candidates.
    assert_eq!(
        a("coalesce(payload.x)", r#"{"x":[]}"#),
        r#"{"a":"undefined"}"#
    );
    assert_eq!(
        a("coalesce(payload.x)", r#"{"x":[null,2]}"#),
        r#"{"a":null}"#
    );
    assert_eq!(a("coalesce(payload.x)", r#"{"x":[[],2]}"#), r#"{"a":[]}"#);
    assert_eq!(
        a("coalesce([payload.x, payload.y])", "{}"),
        r#"{"a":"undefined"}"#
    );
    assert_eq!(a("coalesce_ne(payload.x)", r#"{"x":["",2]}"#), r#"{"a":2}"#);
    assert_eq!(a("coalesce_ne(payload.x)", r#"{"x":[[],2]}"#), r#"{"a":2}"#);
    assert_eq!(
        a("coalesce_ne(payload.x)", r#"{"x":[null,""]}"#),
        r#"{"a":null}"#
    );
    assert_eq!(
        a("coalesce_ne(payload.x)", r#"{"x":[]}"#),
        r#"{"a":"undefined"}"#
    );
    // ... and nothing else.
    for f in ["coalesce", "coalesce_ne"] {
        for payload in [
            "{}",
            r#"{"x":5}"#,
            r#"{"x":"s"}"#,
            r#"{"x":""}"#,
            r#"{"x":{"a":1}}"#,
            r#"{"x":null}"#,
        ] {
            let e = fails(&format!("SELECT {f}(payload.x) AS a FROM \"t/#\""), payload);
            assert!(e.contains("expected an array"), "{f} on {payload}: {e}");
        }
        // No other arity.
        for args in ["", "payload.x, 1, 2"] {
            let e = fails(&format!("SELECT {f}({args}) AS a FROM \"t/#\""), "{}");
            assert!(e.contains("argument(s)"), "{f}({args}): {e}");
        }
    }
    // Two arguments.
    let two = "coalesce(payload.x, payload.y)";
    assert_eq!(a(two, "{}"), r#"{"a":"undefined"}"#);
    assert_eq!(a(two, r#"{"x":null,"y":2}"#), r#"{"a":null}"#);
    let two = "coalesce_ne(payload.x, payload.y)";
    assert_eq!(a(two, r#"{"x":""}"#), r#"{"a":"undefined"}"#);
    assert_eq!(a(two, r#"{"x":[],"y":3}"#), r#"{"a":3}"#);
    assert_eq!(a(two, r#"{"x":null,"y":3}"#), r#"{"a":null}"#);
}

/// The statements mqttd's grammar accepts and EMQX's does not: docs/RULES.md lists each
/// in "Differences from EMQX" as an mqttd-only extension. EMQX 6.3.1 (rulesql 0.2.1)
/// refuses every one of them, probed with `emqx_rule_sqltester:test/1`: `Missing FROM or
/// WHERE` or `syntax error before: …` from the parser (`rulesql.yrl` has `AND`, `OR`,
/// `NOT` and `IN` only under `search_condition`, `CASE` only as a whole field or
/// argument, no parenthesised comparison, and paths only from a `NAME`), `illegal "_"`
/// from the lexer, and `badarg` from `list_to_float/1` for `10e5` and `1.5f`, which
/// `sql_lex.xrl` passes as numbers. They stay accepted, so rules written for mqttd keep
/// loading; every statement EMQX accepts loads here too.
#[test]
fn statements_only_mqttd_accepts_keep_their_meaning() {
    for (sql, payload, want) in ONLY_MQTTD.iter().chain(EMQX_TOO) {
        assert_eq!(one(sql, payload), *want, "{sql}");
    }
}

/// Statements EMQX 6.3.1 refuses and mqttd accepts: statement, payload, output.
const ONLY_MQTTD: &[(&str, &str, &str)] = &[
    // AND / OR / NOT / IN outside WHERE, INCASE and WHEN.
    (
        r#"SELECT payload.a > 1 AND payload.b < 2 AS f FROM "t/#""#,
        r#"{"a":2,"b":1}"#,
        r#"{"f":true}"#,
    ),
    (
        r#"SELECT payload.a > 1 OR payload.b < 2 AS f FROM "t/#""#,
        r#"{"a":0,"b":5}"#,
        r#"{"f":false}"#,
    ),
    (
        r#"SELECT NOT payload.a AS f FROM "t/#""#,
        r#"{"a":true}"#,
        r#"{"f":false}"#,
    ),
    (
        r#"SELECT payload.a IN (1,2) AS f FROM "t/#""#,
        r#"{"a":1}"#,
        r#"{"f":true}"#,
    ),
    (
        r#"SELECT payload.a NOT IN (1,2) AS f FROM "t/#""#,
        r#"{"a":1}"#,
        r#"{"f":false}"#,
    ),
    (
        r#"FOREACH payload.a DO item IN (1,2) AS f FROM "t/#""#,
        r#"{"a":[1]}"#,
        r#"{"f":true}"#,
    ),
    (
        r#"SELECT is_bool(NOT payload.a) AS f FROM "t/#""#,
        r#"{"a":true}"#,
        r#"{"f":true}"#,
    ),
    // A parenthesised comparison, one in an array, and CASE inside an expression.
    (
        r#"SELECT (payload.a > 1) AS f FROM "t/#""#,
        r#"{"a":2}"#,
        r#"{"f":true}"#,
    ),
    (
        r#"SELECT [1, payload.a > 1] AS f FROM "t/#""#,
        r#"{"a":1}"#,
        r#"{"f":[1,false]}"#,
    ),
    (
        r#"SELECT 1 + CASE WHEN true THEN 1 ELSE 2 END AS f FROM "t/#""#,
        "{}",
        r#"{"f":2}"#,
    ),
    // A keyword as a path segment (EMQX wants `payload.'from'`).
    (
        r#"SELECT payload.from AS f FROM "t/#""#,
        r#"{"from":7}"#,
        r#"{"f":7}"#,
    ),
    (
        r#"SELECT payload.in AS f FROM "t/#" WHERE payload.end = 1"#,
        r#"{"in":7,"end":1}"#,
        r#"{"f":7}"#,
    ),
    // Field and index access on a computed value.
    (
        r#"SELECT json_decode(payload).a AS f FROM "t/#""#,
        r#"{"a":7}"#,
        r#"{"f":7}"#,
    ),
    (
        r#"SELECT json_decode(payload)[1] AS f FROM "t/#""#,
        "[7]",
        r#"{"f":7}"#,
    ),
    (
        r#"SELECT (payload).a AS f FROM "t/#""#,
        r#"{"a":7}"#,
        r#"{"f":7}"#,
    ),
    // A name that starts with `_`.
    (
        r#"SELECT payload._x AS _f FROM "t/#""#,
        r#"{"_x":1}"#,
        r#"{"_f":1}"#,
    ),
    // A float with an exponent and no fraction, and the lexer's `f`/`d` suffix.
    (r#"SELECT 1e5 AS f FROM "t/#""#, "{}", r#"{"f":1.0e5}"#),
    (r#"SELECT 10e5 AS f FROM "t/#""#, "{}", r#"{"f":1.0e6}"#),
    (r#"SELECT 1.5f AS f FROM "t/#""#, "{}", r#"{"f":1.5}"#),
    (r#"SELECT 1.5d AS f FROM "t/#""#, "{}", r#"{"f":1.5}"#),
];

/// Their neighbours, which EMQX accepts too, with the same outputs there.
const EMQX_TOO: &[(&str, &str, &str)] = &[
    (
        r#"SELECT payload.a > 1 AS f FROM "t/#""#,
        r#"{"a":2}"#,
        r#"{"f":true}"#,
    ),
    (
        r#"SELECT payload.'from' AS f FROM "t/#""#,
        r#"{"from":7}"#,
        r#"{"f":7}"#,
    ),
    (
        r#"SELECT CASE WHEN payload.a IN (1,2) THEN 1 ELSE 2 END AS f FROM "t/#""#,
        r#"{"a":1}"#,
        r#"{"f":1}"#,
    ),
    (r#"SELECT 1.5e3 AS f FROM "t/#""#, "{}", r#"{"f":1.5e3}"#),
    (
        r#"SELECT is_bool(payload.a > 1) AS f FROM "t/#""#,
        r#"{"a":2}"#,
        r#"{"f":true}"#,
    ),
];

/// EMQX 6.3.1: a value that is not UTF-8 text, here `sprintf('~c', 210)` (the one byte
/// `<<210>>`), is U+FFFD wherever the rule's output is JSON-encoded
/// (`emqx_utils_json:encode/1`: the whole output, `${.}`, a map or list in a template,
/// `json_encode`), and its own bytes in a template (`emqx_template:render/2` of
/// `a${x}b` gives `[<<"a">>, <<210>>, <<"b">>]`).
#[test]
fn text_that_is_not_utf8_is_repaired_in_json_and_exact_in_a_template() {
    let sql = "SELECT sprintf('~c', 210) AS x, [sprintf('~c', 210), 1] AS l FROM \"t/#\"";
    let whole = "{\"x\":\"\u{FFFD}\",\"l\":[\"\u{FFFD}\",1]}";
    assert_eq!(one(sql, "{}"), whole);
    assert_eq!(
        val("json_encode(sprintf('~c', 210))"),
        "\"\\\"\u{FFFD}\\\"\""
    );
    // `str/1` is `emqx_utils_conv:bin/1`, which hands a binary back as it is:
    // `bin2hexstr(str(sprintf('~c', 210)))` is `D2` there.
    assert_eq!(val("bin2hexstr(str(sprintf('~c', 210)))"), r#""D2""#);
    let set = load(
        r#"
        [rules.r]
        sql = '''SELECT sprintf('~c', 210) AS x, [sprintf('~c', 210), 1] AS l FROM "t/#"'''
        actions = [
          { function = "republish", args = { topic = "out", payload = "a${x}b" } },
          { function = "republish", args = { topic = "out", payload = "${l}" } },
          { function = "republish", args = { topic = "out", payload = "${.}" } },
          { function = "console" },
        ]
        "#,
    );
    let payload = Bytes::from_static(b"{}");
    let props = mqtt_core::AppProperties::default();
    let (out, log) = effects(&set, &msg("t/a", &payload, &props));
    assert_eq!(log.len(), 5, "{log:?}");
    assert_eq!(&republished(&out[0].1).payload[..], [b'a', 210, b'b']);
    assert_eq!(
        &republished(&out[1].1).payload[..],
        "[\"\u{FFFD}\",1]".as_bytes()
    );
    assert_eq!(&republished(&out[2].1).payload[..], whole.as_bytes());
    assert!(
        matches!(&out[3].1, Effect::Console(line) if line == whole),
        "{:?}",
        out[3].1
    );
}

#[test]
fn load_errors_are_specific() {
    let e = check_sql("SELECT nope(1) FROM \"t\"").unwrap_err();
    assert!(e.contains("unknown function nope()"), "{e}");
    let e = check_sql("SELECT upper() FROM \"t\"").unwrap_err();
    assert!(e.contains("takes 1 argument"), "{e}");
    let e = check_sql("SELECT a FROM \"t/#/x\"").unwrap_err();
    assert!(e.contains("not a valid topic filter"), "{e}");
    let e = check_sql("SELECT a FROM \"$events/sys/alarm_activated\"").unwrap_err();
    assert!(e.contains("not a supported event"), "{e}");
    let e = check_sql("SELECT a").unwrap_err();
    assert!(e.contains("expected FROM"), "{e}");
    let e = check_sql("SELECT a FROM \"t\" WHERE a = b = c").unwrap_err();
    assert!(e.contains("do not chain"), "{e}");
    let e = check_sql("SELECT a\nFROM \"t\" WHERE a = 'x").unwrap_err();
    assert!(e.contains("line 2"), "{e}");
}

/// A size a function builds from is often a payload field, so it is bounded: past the
/// bound the rule fails (counted, the message still routed) instead of allocating
/// without limit — or panicking in `str::repeat`, which under `panic = "abort"` would
/// end the process.
#[test]
fn payload_supplied_sizes_are_bounded() {
    let pad = "SELECT pad(payload.s, payload.n, 'leading', payload.c) AS p FROM \"t/#\"";
    assert_eq!(one(pad, r#"{"s":"ab","n":5,"c":"0"}"#), r#"{"p":"000ab"}"#);
    // Growth up to 1 MiB beyond the input is allowed; a byte more is not.
    one(pad, r#"{"s":"ab","n":1048578,"c":"0"}"#);
    for n in ["1048579", "4000000000", "9223372036854775807"] {
        let e = fails(pad, &format!(r#"{{"s":"ab","n":{n},"c":"0"}}"#));
        assert!(
            e.contains("more than 1048576 bytes beyond its input"),
            "{n}: {e}"
        );
    }
    // The bound is on bytes built, so a wide pad character cannot multiply it.
    let e = fails(pad, r#"{"s":"ab","n":600000,"c":"ab"}"#);
    assert!(e.contains("beyond its input"), "{e}");

    let f2s = "SELECT float2str(payload.v, payload.d) AS f FROM \"t/#\"";
    one(f2s, r#"{"v":3.25,"d":0}"#);
    one(f2s, r#"{"v":3.25,"d":253}"#);
    for d in ["254", "1000000000", "-1"] {
        let e = fails(f2s, &format!(r#"{{"v":3.25,"d":{d}}}"#));
        assert!(e.contains("decimals must be in 0..=253"), "{d}: {e}");
    }
}

/// Output that grows with the PRODUCT of two payload sizes — a replacement repeated at
/// every match, a separator repeated between items — is refused past 1 MiB of growth,
/// before it is allocated: a 124 KB publish used to ask for 2 GB and abort the process.
/// Ordinary use, including shrinking a large input, is untouched.
#[test]
fn quadratic_string_growth_is_refused_before_it_is_allocated() {
    let big = |c: char, n: usize| c.to_string().repeat(n);
    let replace = "SELECT replace(payload.s, ',', payload.r) AS o FROM \"t/#\"";
    let e = fails(
        replace,
        &format!(
            r#"{{"s":"{}","r":"{}"}}"#,
            big(',', 62_000),
            big('x', 62_000)
        ),
    );
    assert!(e.contains("replace would build more than"), "{e}");
    assert_eq!(
        one(replace, r#"{"s":"a,b,c","r":"; "}"#),
        r#"{"o":"a; b; c"}"#
    );
    // Shrinking a large input is fine whatever its size.
    let shrink = format!(r#"{{"s":"{}","r":""}}"#, big(',', 2_000_000));
    assert_eq!(one(replace, &shrink), r#"{"o":""}"#);

    let rr = "SELECT regex_replace(payload.s, 'a', payload.r) AS o FROM \"t/#\"";
    let e = fails(
        rr,
        &format!(
            r#"{{"s":"{}","r":"{}"}}"#,
            big('a', 20_000),
            big('x', 60_000)
        ),
    );
    assert!(e.contains("regex_replace would build more than"), "{e}");
    // `&` is the whole match: 4,000 copies of a 20,000-byte match is 80 MB.
    let rr_all = "SELECT regex_replace(payload.s, 'a+', payload.r) AS o FROM \"t/#\"";
    let e = fails(
        rr_all,
        &format!(
            r#"{{"s":"{}","r":"{}"}}"#,
            big('a', 20_000),
            big('&', 4_000)
        ),
    );
    assert!(e.contains("regex_replace would build more than"), "{e}");
    assert_eq!(
        one(rr_all, r#"{"s":"xaay","r":"[&]"}"#),
        r#"{"o":"x[aa]y"}"#
    );

    let join = "SELECT join_to_string(payload.sep, payload.items) AS o FROM \"t/#\"";
    let items = format!("[{}]", vec!["\"\""; 20_000].join(","));
    let e = fails(
        join,
        &format!(r#"{{"items":{items},"sep":"{}"}}"#, big('x', 60_000)),
    );
    assert!(e.contains("join_to_string would build more than"), "{e}");
    assert_eq!(
        one(join, r#"{"items":[1,2,3],"sep":"-"}"#),
        r#"{"o":"1-2-3"}"#
    );
}

/// Patterns taken from the payload are compiled once per message and remembered —
/// several of them, so two alternating per `FOREACH` element do not evict each other —
/// and each still matches as its own pattern.
#[test]
fn payload_regex_patterns_are_cached_per_message_and_stay_distinct() {
    let sql = "FOREACH payload.a AS e DO e INCASE regex_match(e, payload.r1) OR regex_match(e, payload.r2) FROM \"t/#\"";
    let out = run_on(
        sql,
        "t/a",
        r#"{"a":["ab","cd","xy","abab","zz"],"r1":"^(ab)+$","r2":"^c"}"#,
    )
    .unwrap();
    assert_eq!(out, [r#"{"e":"ab"}"#, r#"{"e":"cd"}"#, r#"{"e":"abab"}"#]);
}

/// The growth budget is per message, not per call: a `FOREACH` that repeats a large
/// `pad` per element runs out of it, where a per-call bound would have let 256 elements
/// build 256 MiB from one small publish.
#[test]
fn the_growth_budget_is_per_message() {
    let each = "FOREACH payload.a AS e DO pad('x', 600000) AS p FROM \"t/#\"";
    assert_eq!(run_on(each, "t/a", r#"{"a":[1]}"#).unwrap().len(), 1);
    let e = fails(each, r#"{"a":[1,2]}"#);
    assert!(e.contains("budget is per message"), "{e}");
}

/// All of one message's effects together carry at most 4 MiB beyond four times its
/// payload: a `FOREACH` fan-out times several actions times a template repeating the
/// payload no longer turns one publish into a gigabyte of derived messages. Effects up
/// to the budget are kept; past it, each further action fails and is reported.
#[test]
fn a_messages_effects_share_one_byte_budget() {
    let action = |i: usize| {
        format!(
            r#"{{ function = "republish", args = {{ topic = "o/{i}", payload = "${{p}}${{p}}${{p}}${{p}}" }} }}"#
        )
    };
    let text = format!(
        "[rules.fan]\nsql = 'FOREACH payload.a AS e DO payload.p AS p FROM \"t/#\"'\nactions = [{}]\n",
        (1..=4).map(action).collect::<Vec<_>>().join(", ")
    );
    let set = RuleSet::parse(&text).unwrap().rules;
    // An 11 KB publish: 256 elements x 4 actions x a 40 KB rendered payload is 40 MB
    // of derived messages unbounded.
    let payload = Bytes::from(format!(
        r#"{{"a":[{}],"p":"{}"}}"#,
        vec!["0"; 256].join(","),
        "x".repeat(10_000)
    ));
    let props = mqtt_core::AppProperties::default();
    let input = PublishInput::new("c", "t/a", &payload, 0, &props);
    let mut out = Vec::new();
    let mut failed = 0;
    set.on_publish(
        &input,
        &mut |_, o| {
            if matches!(o, Outcome::ActionFailed(_)) {
                failed += 1;
            }
        },
        &mut out,
    );
    let carried: usize = out
        .iter()
        .map(|(_, e)| match e {
            Effect::Republish(r) => r.topic.len() + r.payload.len(),
            Effect::Console(l) => l.len(),
        })
        .sum();
    let limit = MAX_DERIVED_BYTES + 4 * payload.len();
    assert!(carried <= limit, "{carried} > {limit}");
    assert!(
        out.len() > 50 && failed > 0,
        "{} kept, {failed} failed",
        out.len()
    );
    assert_eq!(
        out.len() + failed,
        1024,
        "every action is either kept or reported"
    );
}

/// Each of these crashed the process from a payload value, found by an audit of the
/// functions against publisher-controlled arguments.
#[test]
fn payload_values_cannot_crash_the_evaluator() {
    // A key path of 120,000 segments recursed once per segment: a stack overflow.
    let put = "SELECT map_put(payload.k, 1, map_new()) AS m FROM \"t/#\"";
    let e = fails(put, &format!(r#"{{"k":"{}"}}"#, ".".repeat(120_000)));
    assert!(e.contains("longer than 64"), "{e}");
    assert_eq!(one(put, r#"{"k":"a.b"}"#), r#"{"m":{"a":{"b":1}}}"#);
    let mput = "SELECT mput(payload.k, 1, map_new()) AS m FROM \"t/#\"";
    let keys = format!("[{}]", vec!["\"k\""; 65].join(","));
    assert!(fails(mput, &format!(r#"{{"k":{keys}}}"#)).contains("longer than 64"));

    // A parse-only specifier made the formatter fail, and `to_string()` panicked.
    let fd = "SELECT format_date('second', 'Z', payload.f, 1700000000) AS d FROM \"t/#\"";
    let e = fails(fd, r#"{"f":"%#z"}"#);
    assert!(e.contains("cannot be used to format"), "{e}");
    assert_eq!(one(fd, r#"{"f":"%Y"}"#), r#"{"d":"2023"}"#);

    // At the edge of chrono's range, rendering in an offset panicked inside chrono.
    for ts in ["8210266876799000", "-8334601228800000"] {
        for sql in [
            "SELECT unix_ts_to_rfc3339(payload.ts, 'millisecond') AS t FROM \"t/#\"",
            "SELECT format_date('millisecond', '+14:00', '%Y', payload.ts) AS t FROM \"t/#\"",
            "SELECT format_date('millisecond', '-14:00', '%Y', payload.ts) AS t FROM \"t/#\"",
        ] {
            let e = fails(sql, &format!(r#"{{"ts":{ts}}}"#));
            assert!(e.contains("time out of range"), "{sql} {ts}: {e}");
        }
    }

    // The span of a full i64 range overflowed: a panic with overflow checks on, a
    // wrong (but in-range) answer without them. EMQX's `Min + (N rem Span)` takes the
    // dividend's sign, so -5 maps to -15.
    let mtr = "SELECT map_to_range(payload.n, payload.lo, payload.hi) AS b FROM \"t/#\"";
    assert_eq!(
        one(mtr, r#"{"n":-5,"lo":-10,"hi":9223372036854775807}"#),
        r#"{"b":-15}"#
    );
    one(
        mtr,
        r#"{"n":-5,"lo":-9223372036854775808,"hi":9223372036854775807}"#,
    );
}

/// Work that grew with the square of a payload size: decoding an object with many
/// keys (each insert scanned the map), the user-property map (rebuilt on every
/// reference), and a FOREACH whose INCASE ran for every element of any array.
#[test]
fn payload_shapes_cannot_make_evaluation_quadratic() {
    // 200,000 distinct keys decode in linear time; repeated keys keep their first
    // position and their last value, as before.
    let keys: Vec<String> = (0..200_000).map(|i| format!(r#""k{i}":{i}"#)).collect();
    let payload = format!("{{{}}}", keys.join(","));
    let started = std::time::Instant::now();
    assert_eq!(
        one("SELECT payload.k199999 AS v FROM \"t/#\"", &payload),
        r#"{"v":199999}"#
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let mut dup: Vec<String> = (0..100).map(|i| format!(r#""k{i}":{i}"#)).collect();
    dup.push(r#""k0":"last""#.into());
    assert_eq!(
        one(
            "SELECT json_decode(payload) AS p FROM \"t/#\"",
            &format!("{{{}}}", dup.join(","))
        ),
        format!(
            r#"{{"p":{{{},{}}}}}"#,
            r#""k0":"last""#,
            dup[1..100].join(",")
        )
    );

    // FOREACH iterates at most 10,000 elements, outputs or not.
    let each = "FOREACH payload.a AS e DO e INCASE e > 1 FROM \"t/#\"";
    let arr = |n: usize| format!(r#"{{"a":[{}]}}"#, vec!["0"; n].join(","));
    assert!(run_on(each, "t/a", &arr(10_000)).unwrap().is_empty());
    let e = fails(each, &arr(10_001));
    assert!(e.contains("at most 10000 are iterated"), "{e}");
}

/// Evaluating (and dropping) an expression recurses once per level of its tree, so a
/// tree tall enough to overflow a connection task's stack is refused at load with an
/// error, whether it is built by nesting or by a long left-associative chain — which
/// the parser builds in a loop, without recursing at all.
#[test]
fn expressions_too_deep_to_evaluate_safely_fail_the_load() {
    let chain = |op: &str, n: usize| {
        let terms = vec!["1"; n + 1].join(op);
        format!("SELECT {terms} AS x FROM \"t/#\"")
    };
    let e = check_sql(&chain(" + ", 300)).unwrap_err();
    assert!(e.contains("levels deep"), "{e}");
    let e = check_sql(&chain(" OR ", 300)).unwrap_err();
    assert!(e.contains("levels deep"), "{e}");
    let nots = format!("SELECT a FROM \"t\" WHERE {}a", "NOT ".repeat(300));
    assert!(check_sql(&nots).unwrap_err().contains("levels deep"));
    let parens = format!(
        "SELECT {}1{} AS x FROM \"t\"",
        "(".repeat(100),
        ")".repeat(100)
    );
    assert!(check_sql(&parens).unwrap_err().contains("nests more than"));
    let signs = format!("SELECT {}1 AS x FROM \"t\"", "- ".repeat(100));
    assert!(check_sql(&signs).unwrap_err().contains("nests more than"));
    let calls = format!(
        "SELECT {}a{} AS x FROM \"t\"",
        "abs(".repeat(300),
        ")".repeat(300)
    );
    assert!(check_sql(&calls).is_err());
    // The documented limit exactly: 64 levels of parentheses or signs load, 65 do not.
    let parens_at = |n: usize| {
        format!(
            "SELECT {}1{} AS x FROM \"t/#\"",
            "(".repeat(n),
            ")".repeat(n)
        )
    };
    assert_eq!(one(&parens_at(64), "{}"), r#"{"x":1}"#);
    assert!(check_sql(&parens_at(65))
        .unwrap_err()
        .contains("nests more than 64"));
    let signs_at = |n: usize| format!("SELECT {}1 AS x FROM \"t/#\"", "- ".repeat(n));
    check_sql(&signs_at(64)).unwrap();
    assert!(check_sql(&signs_at(65))
        .unwrap_err()
        .contains("nests more than 64"));

    // Real rules are nowhere near the limits, and evaluate as before.
    check_sql(&chain(" + ", 200)).unwrap();
    assert_eq!(one(&chain(" + ", 200), "{}"), r#"{"x":201}"#);
    assert_eq!(
        one(
            &format!(
                "SELECT {}1{} AS x FROM \"t/#\"",
                "(".repeat(40),
                ")".repeat(40)
            ),
            "{}"
        ),
        r#"{"x":1}"#
    );
}

#[test]
fn double_quoted_comparisons_warn_but_parse_as_fields() {
    // EMQX's grammar: "sensor_1" is a field, not a string.
    let w = check_sql("SELECT * FROM \"t/#\" WHERE payload.name = \"sensor_1\"").unwrap();
    assert_eq!(w.len(), 1);
    assert!(w[0].contains("'sensor_1'"), "{w:?}");
    assert!(run_on(
        "SELECT 1 as a FROM \"t/#\" WHERE payload.name = \"sensor_1\"",
        "t",
        r#"{"name":"sensor_1"}"#
    )
    .unwrap()
    .is_empty());
    assert!(check_sql("SELECT \"my-field\" FROM \"t/#\"")
        .unwrap()
        .is_empty());
}

#[test]
fn comments_and_case_insensitive_keywords() {
    assert_eq!(
        run_on(
            "select clientid -- who\n from \"t/#\" where qos = 1",
            "t",
            "{}"
        )
        .unwrap(),
        [r#"{"clientid":"c_emqx"}"#]
    );
}

#[test]
fn select_star_has_emqx_fields() {
    let out = one("SELECT * FROM \"t/#\"", r#"{"a":1}"#);
    let Value::Map(m) = json_decode(out.as_bytes()).unwrap() else {
        panic!()
    };
    for f in [
        "id",
        "clientid",
        "username",
        "payload",
        "peerhost",
        "peername",
        "topic",
        "qos",
        "flags",
        "pub_props",
        "publish_received_at",
        "event",
        "timestamp",
        "node",
        "metadata",
    ] {
        assert!(m.get(f).is_some(), "SELECT * lacks {f}: {out}");
    }
    assert_eq!(m.get("event").unwrap().as_str(), Some("message.publish"));
    assert_eq!(m.get("peerhost").unwrap().as_str(), Some("127.0.0.1"));
    assert_eq!(
        m.get("metadata").unwrap().to_json().unwrap(),
        r#"{"rule_id":"test"}"#
    );
}

// -- rule sets and actions

fn load(text: &str) -> RuleSet {
    RuleSet::parse(text).unwrap_or_else(|e| panic!("{e}")).rules
}

fn effects(set: &RuleSet, input: &PublishInput) -> (Vec<(Arc<str>, Effect)>, Vec<String>) {
    let mut out = Vec::new();
    let mut log = Vec::new();
    set.on_publish(
        input,
        &mut |r, o| {
            if matches!(o, Outcome::Elapsed(_)) {
                return; // timing, checked on its own
            }
            log.push(format!(
                "{}:{}",
                r.id(),
                match o {
                    Outcome::Elapsed(_) => unreachable!(),
                    Outcome::Passed => "passed".to_string(),
                    Outcome::NoResult => "no_result".to_string(),
                    Outcome::Failed(e) => format!("failed({e})"),
                    Outcome::ActionOk => "action_ok".to_string(),
                    Outcome::ActionFailed(e) => format!("action_failed({e})"),
                    Outcome::Recursive(g) => format!("recursive({})", g.as_str()),
                }
            ));
        },
        &mut out,
    );
    (out, log)
}

fn republished(e: &Effect) -> &Republish {
    match e {
        Effect::Republish(r) => r,
        Effect::Console(c) => panic!("expected a republish, got console {c}"),
    }
}

#[test]
fn republish_defaults_follow_emqx() {
    let set = load(
        r#"
        [rules.copy]
        sql = 'SELECT * FROM "t/#"'
        actions = [{ function = "republish", args = { topic = "copy/${topic}" } }]
        "#,
    );
    // A non-UTF-8 payload is republished byte for byte through `${payload}`.
    let payload = Bytes::from_static(&[0xff, 0x00, 0x01]);
    let props = mqtt_core::AppProperties {
        user_properties: vec![("k".into(), "v".into())],
        ..Default::default()
    };
    let (out, log) = effects(&set, &msg("t/a", &payload, &props));
    assert_eq!(log, ["copy:passed", "copy:action_ok"]);
    let r = republished(&out[0].1);
    assert_eq!(&*out[0].0, "copy");
    assert_eq!(r.topic, "copy/t/a");
    assert_eq!(&r.payload[..], &[0xff, 0x00, 0x01]);
    assert_eq!(r.qos, 1, "qos defaults to ${{qos}}: the original's");
    assert!(
        !r.retain,
        "retain defaults to ${{retain}}, which a publish does not carry"
    );
    assert!(
        r.app.user_properties.is_empty(),
        "user properties are not carried by default (EMQX)"
    );
}

#[test]
fn republish_args_render() {
    let set = load(
        r#"
        [rules.alert]
        sql = '''
        SELECT payload.temp AS temp, clientid, flags.retain AS retain,
               pub_props.'User-Property' AS user_properties
        FROM "sensors/+/data" WHERE payload.temp > 30
        '''
        [[rules.alert.actions]]
        function = "republish"
        [rules.alert.actions.args]
        topic = "alerts/${clientid}"
        qos = 2
        payload = ""
        mqtt_properties = { "Content-Type" = "application/json", "Message-Expiry-Interval" = "60", "Payload-Format-Indicator" = "bogus" }
        "#,
    );
    let payload = Bytes::from_static(br#"{"temp":35}"#);
    let props = mqtt_core::AppProperties {
        user_properties: vec![("site".into(), "a".into())],
        ..Default::default()
    };
    let mut m = msg("sensors/d1/data", &payload, &props);
    m.retain = true;
    let (out, _) = effects(&set, &m);
    let r = republished(&out[0].1);
    assert_eq!(r.topic, "alerts/c_emqx");
    assert_eq!(r.qos, 2);
    assert!(r.retain);
    // An empty payload template is the whole output as JSON.
    assert_eq!(
        std::str::from_utf8(&r.payload).unwrap(),
        r#"{"temp":35,"clientid":"c_emqx","retain":true,"user_properties":{"site":"a"}}"#
    );
    assert_eq!(
        r.app.user_properties,
        [("site".to_string(), "a".to_string())]
    );
    assert_eq!(r.app.content_type.as_deref(), Some("application/json"));
    assert_eq!(r.message_expiry, Some(60));
    assert_eq!(
        r.app.payload_format, None,
        "an unparseable property is dropped, as in EMQX"
    );
    // Below the threshold: no result, no effect.
    let cold = Bytes::from_static(br#"{"temp":3}"#);
    let (out, log) = effects(&set, &msg("sensors/d1/data", &cold, &props));
    assert!(out.is_empty());
    assert_eq!(log, ["alert:no_result"]);
}

#[test]
fn original_user_properties_keep_wire_order_and_duplicates() {
    let set = load(
        r#"
        [rules.r]
        sql = 'SELECT clientid FROM "t"'
        actions = [{ function = "republish", args = { topic = "o", user_properties = "${pub_props.'User-Property'}" } }]
        "#,
    );
    let payload = Bytes::new();
    let props = mqtt_core::AppProperties {
        user_properties: vec![("a".into(), "1".into()), ("a".into(), "2".into())],
        ..Default::default()
    };
    let (out, _) = effects(&set, &msg("t", &payload, &props));
    assert_eq!(
        republished(&out[0].1).app.user_properties,
        props.user_properties
    );
}

#[test]
fn a_bad_rendered_topic_fails_the_action_not_the_rule() {
    let set = load(
        r#"
        [rules.r]
        sql = 'SELECT payload.t AS t FROM "t"'
        actions = [
          { function = "republish", args = { topic = "${t}" } },
          { function = "console" },
        ]
        "#,
    );
    let payload = Bytes::from_static(br#"{"t":"a/+/b"}"#);
    let props = mqtt_core::AppProperties::default();
    let (out, log) = effects(&set, &msg("t", &payload, &props));
    assert_eq!(out.len(), 1, "the console action still ran");
    assert!(matches!(&out[0].1, Effect::Console(c) if c == r#"{"t":"a/+/b"}"#));
    assert_eq!(log[0], "r:passed");
    assert!(
        log[1].starts_with("r:action_failed(rendered topic \"a/+/b\""),
        "{log:?}"
    );
    assert_eq!(log[2], "r:action_ok");
}

#[test]
fn invalid_qos_fails_the_action() {
    let set = load(
        r#"
        [rules.r]
        sql = 'SELECT 7 AS q FROM "t"'
        actions = [{ function = "republish", args = { topic = "x", qos = "${q}" } }]
        "#,
    );
    let payload = Bytes::new();
    let props = mqtt_core::AppProperties::default();
    let (out, log) = effects(&set, &msg("t", &payload, &props));
    assert!(out.is_empty());
    assert!(log[1].contains("qos must be 0, 1 or 2"), "{log:?}");
}

#[test]
fn matching_dedups_orders_and_skips_disabled_rules() {
    let set = load(
        r##"
        [rules.b]
        sql = 'SELECT 1 AS n FROM "a/#", "a/+"'
        actions = [{ function = "console" }]
        [rules.a]
        sql = 'SELECT 2 AS n FROM "a/b"'
        actions = [{ function = "console" }]
        [rules.off]
        sql = 'SELECT 3 AS n FROM "#"'
        enable = false
        actions = [{ function = "console" }]
        "##,
    );
    assert_eq!(set.len(), 3);
    let payload = Bytes::new();
    let props = mqtt_core::AppProperties::default();
    let (out, _) = effects(&set, &msg("a/b", &payload, &props));
    let ids: Vec<&str> = out.iter().map(|(id, _)| &**id).collect();
    assert_eq!(
        ids,
        ["a", "b"],
        "id order, each rule once, disabled rules never"
    );
    let (out, _) = effects(&set, &msg("z", &payload, &props));
    assert!(out.is_empty());

    // Leading wildcards never match a $-topic [MQTT-4.7.2-1]; a filter that names the
    // $ level does. (Enabled rules here: the disabled `#` above proves nothing.)
    let wild = load(
        r##"
        [rules.hash]
        sql = 'SELECT 1 AS n FROM "#"'
        actions = [{ function = "console" }]
        [rules.plus]
        sql = 'SELECT 1 AS n FROM "+/x"'
        actions = [{ function = "console" }]
        [rules.sys]
        sql = 'SELECT 1 AS n FROM "$SYS/#"'
        actions = [{ function = "console" }]
        "##,
    );
    let ids = |topic: &str| -> Vec<String> {
        effects(&wild, &msg(topic, &payload, &props))
            .0
            .iter()
            .map(|(id, _)| id.to_string())
            .collect()
    };
    assert_eq!(
        ids("$SYS/x"),
        ["sys"],
        "only the filter naming $SYS matches it"
    );
    assert_eq!(ids("a/x"), ["hash", "plus"]);
}

#[test]
fn events_are_selected_by_either_emqx_spelling() {
    let set = load(
        r#"
        [rules.online]
        sql = '''SELECT clientid, username, event, proto_ver FROM "$events/client_connected", "$events/client/connected"'''
        actions = [{ function = "republish", args = { topic = "presence/${clientid}", payload = "${.}", qos = 1 } }]
        "#,
    );
    assert!(set.wants_event(EventKind::ClientConnected));
    assert!(!set.wants_event(EventKind::ClientDisconnected));
    assert!(!set.has_message_rules());
    let info = ClientInfo {
        clientid: "dev1",
        username: Some("u"),
        peer: None,
        sockname: None,
        node: "n0",
    };
    let ev = EventInput::client_connected(&info, &ConnInfo::sample(), 1);
    let mut out = Vec::new();
    set.on_event(&ev, &mut |_, _| {}, &mut out);
    assert_eq!(
        out.len(),
        1,
        "both spellings name one event: the rule fires once"
    );
    let r = republished(&out[0].1);
    assert_eq!(r.topic, "presence/dev1");
    assert_eq!(
        std::str::from_utf8(&r.payload).unwrap(),
        r#"{"clientid":"dev1","username":"u","event":"client.connected","proto_ver":5}"#
    );
}

#[test]
fn file_level_validation() {
    let bad = |text: &str, needle: &str| {
        let e = RuleSet::parse(text).unwrap_err().to_string();
        assert!(e.contains(needle), "{e}");
    };
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nbogus = 1",
        "unknown field",
    );
    bad("[rules.1r]\nsql = 'SELECT a FROM \"t\"'", "rule id");
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [\"kafka:sink\"]",
        "no external sinks",
    );
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"republish\" }]",
        "non-empty `topic`",
    );
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"republish\", args = { topic = \"x\", mqtt_properties = { \"Topic-Alias\" = \"1\" } } }]",
        "unsupported mqtt_properties key",
    );
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"webhook\" }]",
        "unsupported action function",
    );
    bad("[rules.r]\nsql = 'SELECT a FROM \"$share/g/t\"'", "$share");
    // EMQX's `union([boolean(), template()])`: a boolean, its text, an empty string (the
    // default) or one placeholder load quietly; another literal loads with a warning,
    // since it is false on every message; anything else is refused.
    for dd in [
        "false",
        "true",
        "\"true\"",
        "\"false\"",
        "\"\"",
        "\"${payload.dd}\"",
    ] {
        let loaded = RuleSet::parse(&format!(
            "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{{ function = \"republish\", args = {{ topic = \"x\", direct_dispatch = {dd} }} }}]"
        ))
        .unwrap();
        assert!(loaded.warnings.is_empty(), "{dd}: {:?}", loaded.warnings);
        assert_eq!(loaded.rules.digest().len(), 64);
    }
    let loaded = RuleSet::parse(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"republish\", args = { topic = \"x\", direct_dispatch = \"yes\" } }]",
    )
    .unwrap();
    assert!(
        loaded.warnings[0]
            .contains("direct_dispatch \"yes\" is neither a boolean nor a placeholder"),
        "{:?}",
        loaded.warnings
    );
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"republish\", args = { topic = \"x\", direct_dispatch = 1 } }]",
        "`direct_dispatch` must be a boolean or a placeholder",
    );
    bad(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"republish\", args = { topic = \"x\", direct_dispatch = \"a${b}\" } }]",
        "direct_dispatch must be a literal or exactly one placeholder",
    );
}

/// docs/RULES.md is the function reference operators read: every built-in must be in it,
/// so a function cannot ship undocumented (or be documented after it is removed — the
/// table would still name it, which this does not catch; the doc's own review does).
#[test]
fn every_function_is_documented_in_rules_md() {
    let doc = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/RULES.md"))
        .expect("docs/RULES.md is readable from the crate");
    for f in function_names() {
        assert!(
            doc.contains(&format!("`{f}`")),
            "docs/RULES.md does not list `{f}`"
        );
    }
}

/// A `qos` or `retain` written as a literal that can never be valid fails the load:
/// otherwise `--check-rules` passes the file and the action fails on every message.
#[test]
fn a_literal_qos_or_retain_that_can_never_be_valid_fails_the_load() {
    let rule = |args: &str| {
        format!(
            "[rules.r]\nsql = 'SELECT * FROM \"t\"'\nactions = [{{ function = \"republish\", \
             args = {{ topic = \"o\", {args} }} }}]\n"
        )
    };
    for bad in [
        "qos = 3",
        "qos = -1",
        "qos = 'high'",
        "retain = 2",
        "retain = 'yes'",
    ] {
        let e = RuleSet::parse(&rule(bad)).unwrap_err().to_string();
        assert!(
            e.contains("qos must be 0, 1 or 2") || e.contains("retain must be a boolean"),
            "{bad}: {e}"
        );
    }
    for good in [
        "qos = 0",
        "qos = 2",
        "qos = '1'",
        "qos = '${payload.q}'",
        "retain = true",
        "retain = 1",
        "retain = 'false'",
        "retain = '${flags.retain}'",
    ] {
        RuleSet::parse(&rule(good)).unwrap_or_else(|e| panic!("{good}: {e}"));
    }
}

/// `mqttd --rule-test` on an event statement runs it against a sample of that event,
/// and an input the broker would never have run the rule on is refused rather than
/// evaluated into a plausible-looking output of undefined fields.
#[test]
fn the_sql_test_simulates_events_and_refuses_inputs_the_rule_never_sees() {
    let c = ClientInfo {
        clientid: "dev-1",
        username: Some("u"),
        peer: Some("127.0.0.1:50000".parse().unwrap()),
        sockname: Some("127.0.0.1:1883".parse().unwrap()),
        node: "n",
    };
    let sql = "SELECT clientid, event, proto_ver, keepalive FROM \"$events/client/connected\"";
    let connected = EventInput::sample(EventKind::ClientConnected, &c, "", 0);
    assert_eq!(
        test_sql(sql, &connected).unwrap(),
        vec![r#"{"clientid":"dev-1","event":"client.connected","proto_ver":5,"keepalive":60}"#]
    );
    let props = mqtt_core::AppProperties::default();
    let payload = Bytes::new();
    let msg = PublishInput::new("dev-1", "t/1", &payload, 0, &props);
    let e = test_sql(sql, &msg).unwrap_err();
    assert!(e.contains("selects only events (client.connected)"), "{e}");
    let subscribed = EventInput::sample(EventKind::SessionSubscribed, &c, "a/#", 1);
    let e = test_sql(sql, &subscribed).unwrap_err();
    assert!(
        e.contains("does not select the session.subscribed event"),
        "{e}"
    );
    let sub = "SELECT topic, qos FROM \"$events/session/subscribed\"";
    assert_eq!(
        test_sql(sub, &subscribed).unwrap(),
        vec![r#"{"topic":"a/#","qos":1}"#]
    );
    assert_eq!(
        statement_sources("SELECT * FROM \"a/#\", \"$events/client/disconnected\"").unwrap(),
        (vec!["a/#".to_string()], vec![EventKind::ClientDisconnected])
    );
}

/// The broker logs a failing rule loudly once per interval PER RULE: a second rule
/// failing at the same time is not hidden behind the first.
#[test]
fn failures_are_reported_once_per_interval_per_rule() {
    let set = RuleSet::parse(
        "[rules.a]\nsql = 'SELECT 1 AS x FROM \"t\"'\n[rules.b]\nsql = 'SELECT 1 AS x FROM \"t\"'\n",
    )
    .unwrap()
    .rules;
    let (a, b) = (&set.rules()[0], &set.rules()[1]);
    assert!(a.failure_report_due(1_000, 10));
    assert!(
        !a.failure_report_due(1_005, 10),
        "within the interval: counted, not reported"
    );
    assert!(
        b.failure_report_due(1_005, 10),
        "another rule is reported on its own"
    );
    assert!(a.failure_report_due(1_010, 10));
}

/// `timestamp` is the message's event time, read once: a statement that selects it
/// twice — raw and formatted, as the cookbook's enrichment recipe does — and a second
/// rule on the same message all see one instant, however long evaluation takes, and
/// it is never earlier than `publish_received_at`.
#[test]
fn a_message_has_one_timestamp_however_often_it_is_read() {
    // The messages below name no value read through `field`: what it returns can be a
    // username, and a test's panic output is a log like any other.
    let int = |v: Value| match v {
        Value::Int(i) => i,
        _ => panic!("a time field is not an integer"),
    };
    let props = mqtt_core::AppProperties::default();
    let payload = Bytes::new();
    let input = PublishInput::new("c", "t/a", &payload, 0, &props);
    let first = int(input.field("timestamp"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while now_ms() <= first {
        assert!(
            std::time::Instant::now() < deadline,
            "the clock never moved"
        );
        std::hint::spin_loop();
    }
    assert!(
        int(input.field("timestamp")) == first,
        "a second read of timestamp moved with the clock"
    );
    let received = int(input.field("publish_received_at"));
    assert!(
        received <= first,
        "timestamp is earlier than publish_received_at"
    );
    let sql =
        "SELECT timestamp AS ms, unix_ts_to_rfc3339(timestamp, 'millisecond') AS at FROM \"t/#\"";
    let out = test_sql(sql, &input).unwrap();
    // The formatted copy is in the host's time zone, so compare the instant it names.
    let row: serde_json::Value = serde_json::from_str(&out[0]).expect("a JSON row");
    let at = chrono::DateTime::parse_from_rfc3339(row["at"].as_str().expect("an RFC 3339 string"))
        .expect("RFC 3339");
    assert!(
        out.len() == 1 && row["ms"].as_i64() == Some(first) && at.timestamp_millis() == first,
        "the raw and formatted timestamps disagree"
    );
    assert!(
        test_sql(sql, &input).unwrap() == out,
        "a second rule saw another instant"
    );

    let mut later = PublishInput::new("c", "t/a", &payload, 0, &props);
    later.received_at_ms = Some(first + 60_000);
    assert!(
        int(later.field("timestamp")) == first + 60_000,
        "timestamp is earlier than a later publish_received_at"
    );
}

// -- ADR 0084: what the admin API, the $SYS reservation and the trace need

/// A rules file with `rules` rules, each matching `regex_match(payload.a, pattern)` for
/// its share of `patterns`.
fn regex_file(patterns: &[String], rules: usize) -> String {
    patterns
        .chunks(patterns.len().div_ceil(rules).max(1))
        .enumerate()
        .map(|(r, chunk)| {
            let conds: Vec<String> = chunk
                .iter()
                .map(|p| format!("regex_match(payload.a, '{p}')"))
                .collect();
            format!(
                "[rules.r{r}]\nsql = '''\nSELECT 1 AS x FROM \"t/#\"\nWHERE {}\n'''\n",
                conds.join(" OR ")
            )
        })
        .collect::<Vec<_>>()
        .concat()
}

/// Every literal pattern used to cost a compile, so a file of a few thousand worst-case
/// patterns took seconds and gigabytes to parse. The budget is per file, across rules,
/// and the pattern past it is refused before it is compiled.
#[test]
fn a_file_may_compile_only_so_many_distinct_regular_expressions() {
    let distinct = |n: usize| -> Vec<String> { (0..n).map(|i| format!("^a{i}$")).collect() };
    let at = RuleSet::parse(&regex_file(&distinct(MAX_REGEX_LITERALS_PER_FILE), 7))
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(at.rules.len(), 7);

    let over = regex_file(&distinct(MAX_REGEX_LITERALS_PER_FILE + 1), 7);
    let e = RuleSet::parse(&over).unwrap_err();
    let LoadError::Rule {
        id,
        message,
        sql_line,
        ..
    } = &e
    else {
        panic!("{e}")
    };
    // The last rule holds the pattern past the budget, on its statement's line 2.
    assert_eq!((id.as_str(), *sql_line), ("r6", Some(2)), "{e}");
    assert!(
        message.contains(&format!(
            "more than {MAX_REGEX_LITERALS_PER_FILE} distinct regular expressions in one rules file"
        )),
        "{e}"
    );

    // Patterns built at run time (from the payload) are not literals: not counted.
    let mut text = regex_file(&distinct(MAX_REGEX_LITERALS_PER_FILE), 7);
    text.push_str(
        "[rules.dyn]\nsql = 'SELECT regex_match(payload.a, payload.p) AS m FROM \"t\"'\n",
    );
    RuleSet::parse(&text).unwrap_or_else(|e| panic!("{e}"));

    // The patterns' bytes are budgeted too: 1 KiB patterns fill it before their count.
    let kib = |n: usize| -> Vec<String> {
        (0..n)
            .map(|i| format!("^{i:04}{}", "b".repeat(1019)))
            .collect()
    };
    let fit = MAX_REGEX_LITERAL_BYTES_PER_FILE / 1024;
    assert!(fit < MAX_REGEX_LITERALS_PER_FILE);
    RuleSet::parse(&regex_file(&kib(fit), 8)).unwrap_or_else(|e| panic!("{e}"));
    let e = RuleSet::parse(&regex_file(&kib(fit + 1), 8)).unwrap_err();
    assert!(
        e.to_string().contains(&format!(
            "more than {MAX_REGEX_LITERAL_BYTES_PER_FILE} bytes of distinct regular expressions"
        )),
        "{e}"
    );
}

/// The most repetitions of `(?:\w\w)` PCRE2 compiles: a repeated group is copied once
/// per repetition, and a compiled pattern holds at most 64K code units (`LINK_SIZE` 2,
/// as in OTP), past which it is `regular expression is too large`.
fn longest_group_run() -> usize {
    let (mut lo, mut hi) = (1_usize, 65_535);
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if funcs::compile_regex(format!("(?:\\w\\w){{{mid}}}").as_bytes()).is_ok() {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

/// An identical pattern is compiled once and counts once, wherever it appears: a file
/// repeating one pattern at PCRE2's size limit far past the budget loads.
#[test]
fn identical_regular_expressions_are_compiled_once() {
    let n = longest_group_run();
    let over = funcs::compile_regex(format!("(?:\\w\\w){{{}}}", n + 1).as_bytes()).unwrap_err();
    assert!(over.contains("regular expression is too large"), "{over}");
    let at_limit = format!("(?:\\w\\w){{{n}}}");
    let mut pool = parser::RegexPool::default();
    let first = pool.get(&at_limit).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &pool.get(&at_limit).unwrap().unwrap()));
    assert!(!Arc::ptr_eq(
        &first,
        &pool
            .get(&format!("(?:\\w\\w){{{}}}", n - 1))
            .unwrap()
            .unwrap()
    ));

    let worst = vec![at_limit; 2 * MAX_REGEX_LITERALS_PER_FILE];
    let set = load(&regex_file(&worst, 32));
    assert_eq!(set.len(), 32);
    // Each still works, and a second distinct pattern is still accepted beside it.
    let mut text = regex_file(&worst, 8);
    text.push_str("[rules.other]\nsql = '''SELECT 1 AS x FROM \"t/#\" WHERE regex_match(payload.a, '^b$')'''\n");
    let set = load(&text);
    let payload = Bytes::from(format!(r#"{{"a":"{}"}}"#, "x".repeat(2 * n)));
    let props = mqtt_core::AppProperties::default();
    let (_, log) = effects(&set, &msg("t/1", &payload, &props));
    let mut want = vec!["other:no_result".to_string()];
    want.extend((0..8).map(|r| format!("r{r}:passed")));
    assert_eq!(log, want);
}

/// The structured error keeps the text `mqttd --check-rules` and a rejected reload have
/// always printed, byte for byte (`crates/mqttd/tests/rules_docs.rs` pins transcripts of
/// it), and adds where: the TOML span in the file, the line and column in a rule's SQL.
#[test]
fn load_errors_say_where_and_print_as_before() {
    let text = "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nbogus = 1\n";
    let e = RuleSet::parse(text).unwrap_err();
    let LoadError::File { message, span } = &e else {
        panic!("{e}")
    };
    assert_eq!(e.to_string(), format!("rules file: {message}"));
    assert!(message.contains("unknown field `bogus`"), "{e}");
    assert_eq!(&text[span.clone().expect("a TOML span")], "bogus");
    assert_eq!(e.file_position(text), Some((3, 1)));
    // The column counts characters, not bytes.
    let text = "[rules.r]\nsql = 'é' x\n";
    assert_eq!(
        RuleSet::parse(text).unwrap_err().file_position(text),
        Some((2, 11))
    );

    let many = (0..=MAX_RULES)
        .map(|i| format!("[rules.r{i}]\nsql = 'SELECT 1 FROM \"t\"'\n"))
        .collect::<Vec<_>>()
        .concat();
    assert_eq!(
        RuleSet::parse(&many).unwrap_err(),
        LoadError::File {
            message: "1025 rules is more than the 1024 a file may define".into(),
            span: None
        }
    );

    let e = RuleSet::parse("[rules.r]\nsql = '''\nSELECT\n  nope(1) FROM \"t\"'''\n").unwrap_err();
    assert_eq!(
        e.to_string(),
        "rule `r`: unknown function nope() — see docs/RULES.md for the supported functions \
         (line 2, column 3, near `nope(1) FROM \"t\"`)"
    );
    assert!(
        matches!(
            &e,
            LoadError::Rule {
                sql_line: Some(2),
                sql_column: Some(3),
                ..
            }
        ),
        "{e:?}"
    );

    // A rule error that is not in the SQL has no position.
    let e = RuleSet::parse(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"webhook\" }]\n",
    )
    .unwrap_err();
    assert_eq!(
        e.to_string(),
        "rule `r`: unsupported action function \"webhook\" (mqttd supports republish and console)"
    );
    assert!(
        matches!(
            &e,
            LoadError::Rule {
                sql_line: None,
                sql_column: None,
                ..
            }
        ),
        "{e:?}"
    );
    let e = RuleSet::parse("[rules.1r]\nsql = 'SELECT a FROM \"t\"'\n").unwrap_err();
    assert_eq!(
        e.to_string(),
        "rule `1r`: a rule id is a letter or `_` followed by up to 63 letters, digits, `_` or `-`"
    );
    let e = RuleSet::load(std::path::Path::new("/nonexistent/rules.toml")).unwrap_err();
    assert!(
        e.to_string()
            .starts_with("rules file: /nonexistent/rules.toml: "),
        "{e}"
    );
}

/// The running set keeps the actions as written and the load's warnings, so the admin
/// API can show a rule and its findings without the file.
#[test]
fn a_loaded_set_keeps_its_action_specs_and_warnings() {
    let loaded = RuleSet::parse(
        r#"
        [rules.r]
        sql = 'SELECT a FROM "t" WHERE a = "x"'
        actions = [
          { function = "republish", args = { topic = "o/${a}", qos = 1, direct_dispatch = false } },
          { function = "console" },
        ]
        [rules.s]
        sql = 'SELECT 1 FROM "u"'
        "#,
    )
    .unwrap();
    assert_eq!(loaded.rules.warnings(), loaded.warnings.as_slice());
    assert_eq!(loaded.warnings.len(), 1, "{:?}", loaded.warnings);
    let r = loaded.rules.get("r").expect("rule r");
    assert_eq!(r.action_specs().len(), 2);
    assert_eq!(
        serde_json::to_value(r.action_specs()).unwrap(),
        serde_json::json!([
            { "function": "republish", "args": { "topic": "o/${a}", "qos": 1, "direct_dispatch": false } },
            { "function": "console" }
        ])
    );
    assert!(loaded.rules.get("s").unwrap().action_specs().is_empty());
    assert!(loaded.rules.get("nope").is_none());
}

/// A dry run evaluates one rule even when it is disabled (the rule being edited), leaves
/// FROM matching to the caller, and gives a non-matching input `--rule-test`'s reason.
#[test]
fn one_rule_can_be_evaluated_whether_or_not_it_is_enabled() {
    let set = load(
        r#"
        [rules.off]
        enable = false
        sql = 'SELECT payload.v AS v FROM "t/+" WHERE v > 1'
        actions = [{ function = "republish", args = { topic = "o/${v}", qos = 0 } }]
        [rules.on]
        sql = 'SELECT clientid FROM "$events/client/connected"'
        "#,
    );
    let payload = Bytes::from_static(br#"{"v":5}"#);
    let props = mqtt_core::AppProperties::default();
    let m = msg("t/1", &payload, &props);
    let (out, _) = effects(&set, &m);
    assert!(out.is_empty(), "the set does not run a disabled rule");

    let mut out = Vec::new();
    let mut log = Vec::new();
    assert!(set.evaluate_one(
        "off",
        &m,
        &mut |r, o| log.push(format!("{}:{}", r.id(), matches!(o, Outcome::Passed))),
        &mut out
    ));
    assert_eq!(log, ["off:true", "off:false"], "passed, then its action");
    assert_eq!(republished(&out[0].1).topic, "o/5");
    assert!(!set.evaluate_one("missing", &m, &mut |_, _| {}, &mut Vec::new()));

    let off = set.get("off").unwrap();
    assert_eq!(off.from_mismatch(&m), None);
    let elsewhere = msg("x/1", &payload, &props);
    let why = off.from_mismatch(&elsewhere).expect("x/1 is not selected");
    let sql = off.sql();
    assert_eq!(Some(why), test_sql(sql, &elsewhere).err());
    let why = set
        .get("on")
        .unwrap()
        .from_mismatch(&m)
        .expect("events only");
    assert!(
        why.contains("selects only events (client.connected)"),
        "{why}"
    );
}

/// Only the broker publishes in `$SYS` (ADR 0084): a republish that renders a topic
/// there fails its action, whatever the template, and the rule's other actions run.
#[test]
fn a_republish_into_sys_fails_its_action() {
    let set = load(
        r#"
        [rules.r]
        sql = 'SELECT payload.t AS t FROM "t"'
        actions = [
          { function = "republish", args = { topic = "${t}" } },
          { function = "republish", args = { topic = "ok/${t}" } },
        ]
        "#,
    );
    let props = mqtt_core::AppProperties::default();
    for reserved in ["$SYS/brokers/n1/rules", "$SYS"] {
        let payload = Bytes::from(format!(r#"{{"t":"{reserved}"}}"#));
        let (out, log) = effects(&set, &msg("t", &payload, &props));
        assert_eq!(
            log[1],
            format!("r:action_failed(republish topic is reserved for the broker: {reserved})")
        );
        assert_eq!(out.len(), 1, "the other action still ran");
        assert_eq!(republished(&out[0].1).topic, format!("ok/{reserved}"));
    }
    // Not reserved: another `$` topic, a different case, or a Mosquitto bridge's state.
    for open in ["$sys/x", "$SYSTEM/x", "$SYS/broker/connection/edge-1/state"] {
        let payload = Bytes::from(format!(r#"{{"t":"{open}"}}"#));
        let (out, _) = effects(&set, &msg("t", &payload, &props));
        assert_eq!(out.len(), 2, "{open}");
    }
}

/// A rule that can never do what it says is loaded, with a warning saying why: a
/// republish whose topic is always in `$SYS`, and a `FROM` on `$SYS`, which the broker's
/// own messages never reach and clients can no longer publish to. A Mosquitto bridge's
/// state is the exception on both sides: a rule may republish there, and a `FROM` that
/// can match it is not warned about.
#[test]
fn rules_aimed_at_sys_load_with_a_warning() {
    let warnings = |actions: &str, from: &str| {
        RuleSet::parse(&format!(
            "[rules.r]\nsql = 'SELECT * FROM {from}'\nactions = [{actions}]\n"
        ))
        .unwrap()
        .warnings
    };
    let republish =
        |topic: &str| format!("{{ function = \"republish\", args = {{ topic = \"{topic}\" }} }}");
    for always in ["$SYS/x/${clientid}", "$SYS/", "$SYS"] {
        let w = warnings(&republish(always), "\"t\"");
        assert_eq!(w.len(), 1, "{always}: {w:?}");
        assert!(
            w[0].starts_with(&format!(
                "rule `r`: republish topic \"{always}\" is in $SYS"
            )),
            "{w:?}"
        );
    }
    // Not always reserved: the warning is for certainties only.
    for maybe in [
        "${t}",
        "$SYS${t}",
        "$SYS/${t}",
        "$sys/x",
        "a/$SYS/b",
        "$SYS/broker/connection/${clientid}/state",
        "$SYS/broker/connection/edge-1/state",
    ] {
        assert!(warnings(&republish(maybe), "\"t\"").is_empty(), "{maybe}");
    }
    for from in [
        "\"$SYS/brokers/#\"",
        "\"$SYS\"",
        "\"a\", \"$SYS/brokers/+/rules\"",
    ] {
        let w = warnings("", from);
        assert_eq!(w.len(), 1, "{from}: {w:?}");
        assert!(w[0].contains("never matches"), "{w:?}");
    }
    for from in [
        "\"$sys/#\", \"#\"",
        "\"$SYS/#\"",
        "\"$SYS/broker/connection/+/state\"",
    ] {
        assert!(warnings("", from).is_empty(), "{from}");
    }
    let w = check_sql("SELECT * FROM \"$SYS/brokers/#\"").unwrap();
    assert!(
        w[0].starts_with("FROM \"$SYS/brokers/#\" never matches"),
        "{w:?}"
    );
}

/// The trace records at most `per_sec` evaluations of a rule a second; the window is
/// per rule, resets each second, and `no_result` has its own so it cannot starve the
/// passed records of a rule whose WHERE rarely passes.
#[test]
fn the_trace_rate_window_is_per_rule_per_second() {
    let set = load(
        "[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\n[rules.b]\nsql = 'SELECT 1 FROM \"t\"'\n\
         [rules.c]\nsql = 'SELECT 1 FROM \"t\"'\n",
    );
    let (a, b, c) = (
        set.get("a").unwrap(),
        set.get("b").unwrap(),
        set.get("c").unwrap(),
    );
    let t = 1_800_000_000;
    let taken = |r: &Rule, now: u64, n: usize| (0..n).filter(|_| r.trace_due(now, 3)).count();
    let no_result =
        |r: &Rule, now: u64, n: usize| (0..n).filter(|_| r.no_result_trace_due(now, 2)).count();
    assert_eq!(taken(a, t, 10), 3, "three in the second, then none");
    assert_eq!(taken(b, t, 10), 3, "another rule has its own window");
    assert_eq!(no_result(a, t, 10), 2, "no_result's window is its own");
    assert_eq!(taken(a, t + 1, 10), 3, "the next second opens a new window");
    assert_eq!(no_result(a, t + 1, 10), 2);
    assert_eq!(
        taken(a, t, 10),
        0,
        "a clock read a second behind counts against the newer, open window"
    );
    assert_eq!(
        taken(a, t - 60, 10),
        3,
        "a clock stepped back opens its own"
    );
    assert!(!b.trace_due(t + 2, 0), "a rate of zero records nothing");
    // The second is kept in 32 bits; the window still turns over where they wrap.
    let wrap = 1 << 32;
    assert_eq!(taken(c, wrap - 2, 10), 3);
    assert_eq!(taken(c, wrap - 1, 10), 3);
    assert_eq!(taken(c, wrap, 10), 3);
    assert_eq!(taken(c, wrap - 1, 10), 0);
}

/// An [`Outcome`]'s name, for asserting on a sequence of them.
fn outcome_kind(o: Outcome<'_>) -> &'static str {
    match o {
        Outcome::Passed => "passed",
        Outcome::NoResult => "no_result",
        Outcome::Failed(_) => "failed",
        Outcome::ActionOk => "action_ok",
        Outcome::ActionFailed(_) => "action_failed",
        Outcome::Elapsed(_) => "elapsed",
        Outcome::Recursive(Recursion::SameRule) => "recursive_same_rule",
        Outcome::Recursive(Recursion::Depth) => "recursive_depth",
    }
}

/// Each rule a message reaches reports how long it took (ADR 0084), once and last, after
/// its other outcomes, whether it passed, filtered or failed; a rule it does not reach,
/// and a dry run, report none.
#[test]
fn every_evaluated_rule_reports_its_time_once_after_its_outcomes() {
    let set = load(
        r#"
        [rules.pass]
        sql = 'SELECT payload.v AS v FROM "t/+"'
        actions = [{ function = "republish", args = { topic = "o/${v}", qos = 0 } }]
        [rules.filter]
        sql = 'SELECT payload.v AS v FROM "t/+" WHERE v > 100'
        [rules.fail]
        sql = 'SELECT payload.v + "x" AS v FROM "t/+"'
        [rules.elsewhere]
        sql = 'SELECT payload FROM "x/+"'
        "#,
    );
    let payload = Bytes::from_static(br#"{"v":5}"#);
    let props = mqtt_core::AppProperties::default();
    let m = msg("t/1", &payload, &props);
    let mut log: Vec<(String, &'static str)> = Vec::new();
    set.on_publish(
        &m,
        &mut |r, o| log.push((r.id().to_string(), outcome_kind(o))),
        &mut Vec::new(),
    );
    let mut per_rule: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for (id, kind) in &log {
        per_rule.entry(id.clone()).or_default().push(kind);
    }
    assert_eq!(
        per_rule,
        BTreeMap::from([
            ("fail".to_string(), vec!["failed", "elapsed"]),
            ("filter".to_string(), vec!["no_result", "elapsed"]),
            ("pass".to_string(), vec!["passed", "action_ok", "elapsed"]),
        ]),
        "{log:?}"
    );

    let mut dry = Vec::new();
    assert!(set.evaluate_one(
        "pass",
        &m,
        &mut |_, o| dry.push(outcome_kind(o)),
        &mut Vec::new()
    ));
    assert!(!dry.contains(&"elapsed"), "a dry run is not timed: {dry:?}");
}

/// The client every event below is about: one with a username, an address and a
/// listener, so every field EMQX's builder can set is set.
fn event_client() -> ClientInfo<'static> {
    ClientInfo {
        clientid: "c_emqx",
        username: Some("u_emqx"),
        peer: Some("192.168.0.10:56431".parse().unwrap()),
        sockname: Some("10.0.0.1:1883".parse().unwrap()),
        node: "n0",
    }
}

/// An event's field names, sorted.
fn field_names(ev: &EventInput) -> Vec<String> {
    let mut names: Vec<String> = ev.all_fields().iter().map(|(k, _)| k.to_string()).collect();
    names.sort();
    names
}

fn json_of(ev: &EventInput, field: &str) -> String {
    ev.field(field).to_json().unwrap()
}

fn no_props() -> Map {
    printable_props::<&str, &str>(&[], [])
}

/// Every event carries exactly the fields EMQX's builder for it sets
/// (`emqx_rule_events:eventmsg_*` plus `with_basic_columns/3`'s `event`, `timestamp` and
/// `node`; emqx/emqx `apps/emqx_rule_engine/src/emqx_rule_events.erl`) — no more, and
/// none missing but `mountpoint`, which mqttd has no feature for. A field dropped from a
/// builder, or added to the wrong event, fails here.
// One table row per event: long by the number of fields EMQX's builders set.
#[allow(clippy::too_many_lines)]
#[test]
fn every_event_carries_exactly_emqx_s_fields() {
    let c = event_client();
    let conn = ConnInfo::sample();
    let cases: Vec<(EventInput, &[&str])> = vec![
        (
            // eventmsg_connected/2
            EventInput::client_connected(&c, &conn, 1),
            &[
                "clean_start",
                "client_attrs",
                "clientid",
                "conn_props",
                "connected_at",
                "event",
                "expiry_interval",
                "is_bridge",
                "keepalive",
                "node",
                "peername",
                "proto_name",
                "proto_ver",
                "receive_maximum",
                "sockname",
                "timestamp",
                "username",
            ],
        ),
        (
            // eventmsg_disconnected/3
            EventInput::client_disconnected(&c, 5, "normal", no_props(), 1),
            &[
                "client_attrs",
                "clientid",
                "connected_at",
                "disconn_props",
                "disconnected_at",
                "event",
                "node",
                "peername",
                "proto_name",
                "proto_ver",
                "reason",
                "sockname",
                "timestamp",
                "username",
            ],
        ),
        (
            // eventmsg_connack/2
            EventInput::client_connack(&c, &conn, "success", Some(1)),
            &[
                "clean_start",
                "clientid",
                "conn_props",
                "connected_at",
                "event",
                "expiry_interval",
                "keepalive",
                "node",
                "peername",
                "proto_name",
                "proto_ver",
                "reason_code",
                "sockname",
                "timestamp",
                "username",
            ],
        ),
        (
            // eventmsg_ping/2
            EventInput::client_ping(&c, &conn),
            &[
                "clean_start",
                "clientid",
                "conn_props",
                "event",
                "expiry_interval",
                "keepalive",
                "node",
                "peername",
                "proto_name",
                "proto_ver",
                "sockname",
                "timestamp",
                "username",
            ],
        ),
        (
            // eventmsg_check_authn_complete/2
            EventInput::check_authn_complete(&c, "success", false),
            &[
                "client_attrs",
                "clientid",
                "event",
                "is_anonymous",
                "is_superuser",
                "node",
                "peername",
                "reason_code",
                "timestamp",
                "username",
            ],
        ),
        (
            // eventmsg_check_authz_complete/5
            EventInput::check_authz_complete(&c, "t/1", "publish", "file", true),
            &[
                "action",
                "authz_source",
                "client_attrs",
                "clientid",
                "event",
                "node",
                "peerhost",
                "peername",
                "result",
                "timestamp",
                "topic",
                "username",
            ],
        ),
        (
            // eventmsg_sub_or_unsub/4, session.subscribed
            EventInput::session_subscribed(&c, "t/#", 1, no_props()),
            &[
                "client_attrs",
                "clientid",
                "event",
                "node",
                "peerhost",
                "peername",
                "qos",
                "sub_props",
                "timestamp",
                "topic",
                "username",
            ],
        ),
        (
            // eventmsg_sub_or_unsub/4, session.unsubscribed
            EventInput::session_unsubscribed(&c, "t/#", 1, no_props()),
            &[
                "client_attrs",
                "clientid",
                "event",
                "node",
                "peerhost",
                "peername",
                "qos",
                "timestamp",
                "topic",
                "unsub_props",
                "username",
            ],
        ),
    ];
    for (ev, want) in cases {
        assert_eq!(field_names(&ev), want, "{}", ev.kind().event_name());
        assert_eq!(
            ev.field("event").as_str(),
            Some(ev.kind().event_name()),
            "the event field names the hook"
        );
    }
}

/// The values EMQX's builders put in those fields where its builders differ from one
/// another: `expiry_interval` is seconds on `client.connected` (EMQX divides its
/// milliseconds by 1000 there) but milliseconds on `client.connack` and `client.ping`;
/// addresses print as `emqx_utils:ntoa/1` does; `result`, `action` and `reason_code` are
/// strings; `is_superuser` is always false in mqttd.
#[test]
fn event_values_follow_emqx_s_builders() {
    let c = event_client();
    let conn = ConnInfo {
        proto_ver: 4,
        keepalive: 30,
        clean_start: false,
        expiry_interval: 7200,
        receive_maximum: 32,
        conn_props: no_props(),
    };
    let connected = EventInput::client_connected(&c, &conn, 7);
    assert_eq!(json_of(&connected, "expiry_interval"), "7200");
    assert_eq!(json_of(&connected, "receive_maximum"), "32");
    assert_eq!(json_of(&connected, "proto_name"), r#""MQTT""#);
    assert_eq!(json_of(&connected, "proto_ver"), "4");
    assert_eq!(json_of(&connected, "is_bridge"), "false");
    assert_eq!(json_of(&connected, "peername"), r#""192.168.0.10:56431""#);
    assert_eq!(json_of(&connected, "sockname"), r#""10.0.0.1:1883""#);
    assert_eq!(json_of(&connected, "conn_props"), r#"{"User-Property":{}}"#);
    assert_eq!(json_of(&connected, "client_attrs"), "{}");
    for ev in [
        EventInput::client_connack(&c, &conn, "success", Some(7)),
        EventInput::client_ping(&c, &conn),
    ] {
        assert_eq!(
            json_of(&ev, "expiry_interval"),
            "7200000",
            "{:?}",
            ev.kind()
        );
        assert_eq!(json_of(&ev, "clean_start"), "false");
    }
    let refused = EventInput::client_connack(&c, &conn, "not_authorized", None);
    assert_eq!(json_of(&refused, "reason_code"), r#""not_authorized""#);
    assert!(
        refused.field("connected_at").is_undefined(),
        "only a success connected"
    );

    let authz = EventInput::check_authz_complete(&c, "t/1", "subscribe", "default", false);
    assert_eq!(json_of(&authz, "result"), r#""deny""#);
    assert_eq!(json_of(&authz, "action"), r#""subscribe""#);
    assert_eq!(json_of(&authz, "authz_source"), r#""default""#);
    assert_eq!(json_of(&authz, "peerhost"), r#""192.168.0.10""#);
    let failed = EventInput::check_authn_complete(&c, "bad_username_or_password", true);
    assert_eq!(
        json_of(&failed, "reason_code"),
        r#""bad_username_or_password""#
    );
    assert_eq!(json_of(&failed, "is_anonymous"), "true");
    assert_eq!(json_of(&failed, "is_superuser"), "false");

    let unsub = EventInput::session_unsubscribed(&c, "t/#", 2, no_props());
    assert_eq!(json_of(&unsub, "qos"), "2");
    assert_eq!(json_of(&unsub, "unsub_props"), r#"{"User-Property":{}}"#);

    // A client that sent no username: EMQX's builders still set the key, to `undefined`.
    let anonymous = ClientInfo {
        username: None,
        ..c
    };
    let ev = EventInput::check_authn_complete(&anonymous, "success", true);
    assert!(field_names(&ev).contains(&"username".to_string()));
    assert_eq!(json_of(&ev, "username"), r#""undefined""#);
}

/// `emqx_utils:ntoa/1`: an IPv6 peer prints unbracketed, and an IPv4-mapped one as the
/// IPv4 address it carries — on events and on messages alike.
#[test]
fn addresses_print_as_emqx_prints_them() {
    assert_eq!(ntoa("[::1]:1883".parse().unwrap()), "::1:1883");
    assert_eq!(
        ntoa("[::ffff:10.1.2.3]:5000".parse().unwrap()),
        "10.1.2.3:5000"
    );
    assert_eq!(ntoa("127.0.0.1:1883".parse().unwrap()), "127.0.0.1:1883");
    let props = mqtt_core::AppProperties::default();
    let payload = Bytes::new();
    let mut m = PublishInput::new("c", "t", &payload, 0, &props);
    m.peer = Some("[::ffff:10.1.2.3]:5000".parse().unwrap());
    assert_eq!(m.field("peerhost").as_str(), Some("10.1.2.3"));
    assert_eq!(m.field("peername").as_str(), Some("10.1.2.3:5000"));
}

/// `emqx_utils_maps:printable_props/1`: `User-Property` is always there (a map, the last
/// value of a repeated key), `User-Property-Pairs` keeps every pair in order when there
/// was one, and the other properties keep their names.
#[test]
fn property_maps_print_as_emqx_prints_them() {
    assert_eq!(
        Value::from(no_props()).to_json().unwrap(),
        r#"{"User-Property":{}}"#
    );
    let m = printable_props(
        &[("k", "1"), ("k", "2")],
        [("Session-Expiry-Interval", Value::Int(7200))],
    );
    assert_eq!(
        Value::from(m).to_json().unwrap(),
        r#"{"User-Property":{"k":"2"},"User-Property-Pairs":[{"key":"k","value":"1"},{"key":"k","value":"2"}],"Session-Expiry-Interval":7200}"#
    );
}

/// EMQX evaluates the publish hook on `emqx_message:clean_dup(Msg)`
/// (`emqx_broker:publish/1`), so a rule always reads `flags.dup` as `false`.
#[test]
fn a_rule_never_sees_the_dup_flag() {
    assert_eq!(
        one("SELECT flags FROM \"t/#\"", "{}"),
        r#"{"flags":{"dup":false,"retain":false}}"#
    );
}

/// EMQX's reason-code vocabulary (`emqx_reason_codes:name/1`) and its disconnect reason
/// (`emqx_channel:disconnect_reason/1`: `normal` for `0x00`, the name otherwise).
#[test]
fn reason_codes_are_named_as_emqx_names_them() {
    assert_eq!(reason_code_name(0x00), "success");
    assert_eq!(disconnect_reason(0x00), "normal");
    for (code, name) in [
        (0x04, "disconnect_with_will_message"),
        (0x82, "protocol_error"),
        (0x85, "client_identifier_not_valid"),
        (0x86, "bad_username_or_password"),
        (0x87, "not_authorized"),
        (0x8C, "bad_authentication_method"),
        (0x8D, "keepalive_timeout"),
        (0x90, "topic_name_invalid"),
        (0x93, "receive_maximum_exceeded"),
        (0x94, "topic_alias_invalid"),
        (0x97, "quota_exceeded"),
        (0x9C, "use_another_server"),
        (0x03, "unknown_error"),
    ] {
        assert_eq!(reason_code_name(code), name, "{code:#04x}");
        assert_eq!(disconnect_reason(code), name, "{code:#04x}");
    }
}

/// EMQX's event topics (`emqx_rule_events:event_topics_enum/0`): each new event in both
/// spellings where EMQX has both, and `client/ping` only namespaced; a `FROM` names it by
/// either, and `EventKind::parse` by its hook name too.
#[test]
fn the_new_events_are_named_as_emqx_names_them() {
    for (topic, kind) in [
        ("$events/client/connack", EventKind::ClientConnack),
        ("$events/client_connack", EventKind::ClientConnack),
        ("$events/client/ping", EventKind::ClientPing),
        (
            "$events/auth/check_authn_complete",
            EventKind::CheckAuthnComplete,
        ),
        (
            "$events/client_check_authn_complete",
            EventKind::CheckAuthnComplete,
        ),
        (
            "$events/auth/check_authz_complete",
            EventKind::CheckAuthzComplete,
        ),
        (
            "$events/client_check_authz_complete",
            EventKind::CheckAuthzComplete,
        ),
    ] {
        assert_eq!(EventKind::from_topic(topic), Some(kind), "{topic}");
        assert_eq!(EventKind::parse(topic), Some(kind), "{topic}");
        assert_eq!(EventKind::parse(kind.event_name()), Some(kind));
        assert_eq!(EventKind::from_topic(kind.topic()), Some(kind));
    }
    assert_eq!(
        EventKind::from_topic("$events/client_ping"),
        None,
        "EMQX has no alias"
    );
    assert_eq!(EventKind::ALL.len(), EventKind::COUNT);
    for (i, k) in EventKind::ALL.iter().enumerate() {
        assert_eq!(k.index(), i);
    }
}

/// EMQX matches a `FROM "$events/…"` filter against its event topics with
/// `emqx_topic:match/2` (`match_event_names/1`): a wildcard selects every event it
/// matches, in either spelling, once. One matching only events mqttd does not raise is
/// refused; one also matching those loads, with a warning naming them.
#[test]
fn wildcard_event_filters_select_what_emqx_s_match_selects() {
    let m = EventKind::matching("$events/client/+");
    assert_eq!(
        m.kinds,
        [
            EventKind::ClientConnected,
            EventKind::ClientDisconnected,
            EventKind::ClientConnack,
            EventKind::ClientPing
        ]
    );
    assert!(m.unsupported.is_empty());
    assert_eq!(
        EventKind::matching("$events/auth/#").kinds,
        [EventKind::CheckAuthnComplete, EventKind::CheckAuthzComplete]
    );
    assert_eq!(
        EventKind::matching("$events/+").kinds,
        [
            EventKind::ClientConnected,
            EventKind::ClientDisconnected,
            EventKind::ClientConnack,
            EventKind::CheckAuthnComplete,
            EventKind::CheckAuthzComplete,
            EventKind::SessionSubscribed,
            EventKind::SessionUnsubscribed,
            EventKind::MessageDelivered,
            EventKind::MessageAcked,
            EventKind::MessageDropped,
            EventKind::DeliveryDropped
        ],
        "the underscore spellings are one level; ping has none"
    );
    assert_eq!(
        EventKind::matching("$events/message/+").kinds,
        EventKind::MESSAGE,
        "the four message events, and nothing mqttd does not raise"
    );
    assert!(EventKind::matching("$events/message/+")
        .unsupported
        .is_empty());
    let all = EventKind::matching("$events/#");
    assert_eq!(all.kinds, EventKind::ALL);
    assert!(all.unsupported.contains(&"$events/sys/alarm_activated"));

    let set = load(
        r#"
        [rules.clients]
        sql = 'SELECT clientid FROM "$events/client/+"'
        actions = [{ function = "console" }]
        "#,
    );
    for k in EventKind::ALL {
        assert_eq!(
            set.wants_event(k),
            k.topic().starts_with("$events/client/"),
            "{k:?}"
        );
    }
    let loaded = RuleSet::parse(
        "[rules.all]\nsql = 'SELECT clientid FROM \"$events/#\"'\nactions = [{ function = \"console\" }]\n",
    )
    .unwrap();
    assert!(EventKind::ALL.iter().all(|k| loaded.rules.wants_event(*k)));
    let (unraised, selected) = loaded.warnings[0]
        .split_once("; it selects")
        .expect("the warning names both lists");
    assert!(
        unraised.contains("also matches events mqttd does not raise")
            && unraised.contains("$events/sys/alarm_activated")
            && !unraised.contains("$events/message/"),
        "{:?}",
        loaded.warnings
    );
    assert!(selected.contains("$events/message/delivered"), "{selected}");
    for (sql, needle) in [
        (
            "SELECT * FROM \"$events/sys/+\"",
            "matches no event mqttd raises",
        ),
        (
            "SELECT * FROM \"$events/client/#/x\"",
            "is not a valid topic filter",
        ),
        (
            "SELECT * FROM \"$sources/mqtt:in\"",
            "no data integration sources",
        ),
        (
            "SELECT * FROM \"$events/client/pong\"",
            "is not a supported event",
        ),
    ] {
        let e = RuleSet::parse(&format!("[rules.r]\nsql = '{sql}'\n"))
            .unwrap_err()
            .to_string();
        assert!(e.contains(needle), "{sql}: {e}");
    }
}

/// A rule selecting one of the new events runs on it, and only on it.
#[test]
fn the_new_events_run_the_rules_that_select_them() {
    let set = load(
        r#"
        [rules.denied]
        sql = '''SELECT clientid, topic, action FROM "$events/auth/check_authz_complete" WHERE result = 'deny' '''
        actions = [{ function = "console" }]
        "#,
    );
    assert!(set.wants_event(EventKind::CheckAuthzComplete));
    assert!(!set.wants_event(EventKind::ClientPing));
    let c = event_client();
    let mut out = Vec::new();
    for allowed in [true, false] {
        let ev = EventInput::check_authz_complete(&c, "t/1", "publish", "file", allowed);
        set.on_event(&ev, &mut |_, _| {}, &mut out);
    }
    assert_eq!(out.len(), 1, "only the denial passes the WHERE");
    assert!(matches!(
        &out[0].1,
        Effect::Console(json) if json == r#"{"clientid":"c_emqx","topic":"t/1","action":"publish"}"#
    ));
}

/// `expr` fails the rule, as it fails EMQX's.
fn refused(expr: &str) {
    let sql = format!("SELECT {expr} AS r FROM \"t/#\"");
    assert!(
        run_on(&sql, "t/a", "{}").is_err(),
        "{expr} should fail the rule, as it does in EMQX"
    );
}

/// EMQX's string functions call Erlang's `string` module, which counts and matches
/// grapheme clusters (Unicode 16), finds a match by code point and then requires the
/// cluster that STARTS there to be the pattern's last character. Every value below is
/// EMQX 6.3.1's (`emqx_rule_sqltester:test/1` on the same SQL).
#[test]
fn string_functions_work_on_grapheme_clusters_like_emqx() {
    // strlen / pad / substr count clusters: `\r\n`, a flag, a jamo syllable, a ZWJ family
    // and an Indic conjunct (GB9c) are one each.
    assert_eq!(val(r"strlen(unescape('a\r\nb'))"), "3");
    assert_eq!(val("strlen('🇪🇸')"), "1");
    assert_eq!(val("strlen('héllo')"), "5");
    assert_eq!(val("strlen('\u{1100}\u{1161}\u{11A8}')"), "1");
    assert_eq!(val("strlen('👨\u{200D}👩\u{200D}👧')"), "1");
    assert_eq!(val("strlen('क्षि')"), "1");
    // The tables are Unicode 16's, as OTP 28's are. U+1ACF is unassigned there, so it is a
    // cluster of its own after `a`; Unicode 17 makes it a combining mark and the pair one
    // cluster. This fails if `unicode-segmentation` moves past 1.12 (see Cargo.toml).
    assert_eq!(val("strlen('a\u{1ACF}')"), "2");
    assert_eq!(val("pad('🇪🇸', 4)"), r#""🇪🇸   ""#);
    assert_eq!(val("pad('ab', 7, 'both', 'xy')"), r#""xyxyabxyxyxy""#);
    assert_eq!(val("pad('ab', -1)"), r#""ab""#);
    assert_eq!(val(r"substr(unescape('a\r\nbc'), 1, 2)"), r#""\r\nb""#);
    assert_eq!(val("substr('abc', 5)"), r#""""#);
    refused("substr('abc', 0, -1)");
    refused("substr('abc', -1)");
    // reverse writes each character as one byte: Latin-1 for U+0080..U+00FF, a failure
    // above, and a combining sequence fails too (its mark is above U+00FF).
    assert_eq!(val(r"reverse(unescape('a\r\nb'))"), r#""b\r\na""#);
    assert_eq!(val("bin2hexstr(reverse('aé'))"), r#""E961""#);
    refused("reverse('a€')");
    refused("reverse('e\u{301}')");
    // trim removes Pattern_White_Space (with \r\n as one cluster): not a no-break space,
    // but a left-to-right mark and NEL.
    assert_eq!(val(r"trim(unescape('\r\n a \r\n'))"), r#""a""#);
    assert_eq!(val("trim('\u{a0}a\u{a0}')"), "\"\u{a0}a\u{a0}\"");
    assert_eq!(val("trim('\u{200e} a \u{200f}')"), r#""a""#);
    assert_eq!(val("ltrim('\u{85} a')"), r#""a""#);
    // rtrim/2: `\n` alone trims the end of a `\r\n` (the cluster starting at `\n` is
    // `\n`), while `\r` and `\n` as two separators do not match the cluster `\r\n`.
    assert_eq!(
        val(r"rtrim(unescape('ab\r\n'), unescape('\n'))"),
        r#""ab\r""#
    );
    assert_eq!(
        val(r"rtrim(unescape('ab\r\n'), unescape('\r\n'))"),
        r#""ab\r\n""#
    );
    assert_eq!(val("rtrim('abcxxyx', 'xy')"), r#""abc""#);
    assert_eq!(val("rtrim('ae\u{301}', 'e')"), "\"ae\u{301}\"");
    assert_eq!(val("rtrim('ae\u{301}', '\u{301}')"), r#""ae""#);
    // find / split / replace.
    assert_eq!(val(r"find(unescape('a\r\nb'), unescape('\r'))"), r#""""#);
    assert_eq!(val(r"find(unescape('a\r\nb'), unescape('\n'))"), r#""\nb""#);
    assert_eq!(
        val(r"find(unescape('a\r\nb'), unescape('\r\n'))"),
        r#""\r\nb""#
    );
    assert_eq!(val("find('aaa', 'aa', 'trailing')"), r#""aa""#);
    assert_eq!(val("find('abc', '', 'trailing')"), r#""abc""#);
    assert_eq!(
        val(r"split(unescape('a\r\nb'), unescape('\n'))"),
        r#"["a\r","b"]"#
    );
    assert_eq!(
        val(r"split(unescape('a\r\nb'), unescape('\r'))"),
        r#"["a\r\nb"]"#
    );
    assert_eq!(val("split('aaaa', 'aa', 'notrim')"), r#"["","",""]"#);
    assert_eq!(val("split('aaa', 'aa', 'trailing_notrim')"), r#"["a",""]"#);
    assert_eq!(val("split('xe\u{301}yez', 'e')"), "[\"xe\u{301}y\",\"z\"]");
    assert_eq!(val("replace('xe\u{301}yez', 'e', 'E')"), "\"xe\u{301}yEz\"");
    assert_eq!(val("replace('aaaa', 'aa', 'b')"), r#""bb""#);
    assert_eq!(val("replace('a.b.c', '.', '-', 'trailing')"), r#""a.b-c""#);
    // tokens works on BYTES (`binary_to_list`): each separator byte separates, and
    // `\r\n` is a cluster no single separator matches.
    assert_eq!(
        val(r"tokens(unescape('a\r\nb\rc'), unescape('\r'))"),
        r#"["a\r\nb","c"]"#
    );
    assert_eq!(
        val(r"tokens(unescape('a\r\nb'), unescape('\n'))"),
        r#"["a\r","b"]"#
    );
    assert_eq!(
        val(r"tokens(unescape('a\r\nb'), unescape('\r\n'))"),
        r#"["a\r\nb"]"#
    );
    assert_eq!(
        val(r"tokens(unescape('a\r\nb\rc\nd'), ',', 'nocrlf')"),
        r#"["a","b","c","d"]"#
    );
    assert_eq!(val("tokens('a b', '')"), r#"["a b"]"#);
    assert_eq!(val("tokens('', ' ')"), "[]");
    // Case mapping is per code point: a final sigma stays σ.
    assert_eq!(val("lower('ΑΣ')"), r#""ασ""#);
    assert_eq!(val("upper('ß')"), r#""SS""#);
    // ascii is the first BYTE; the atoms true/false are strings to these functions.
    assert_eq!(val("ascii('é')"), "195");
    refused("ascii('')");
    assert_eq!(val("ascii(true)"), "116");
    assert_eq!(val("reverse(true)"), r#""eurt""#);
    assert_eq!(val("upper(false)"), r#""FALSE""#);
    assert_eq!(val("rm_prefix(true, 't')"), r#""rue""#);
    refused("strlen(json_decode('null'))");
    // unescape keeps a backslash that ends the string.
    assert_eq!(val(r"unescape('a\')"), r#""a\\""#);
}

/// `regex_replace`'s replacement is Erlang `re:replace`'s, as OTP's `precomp_repl/1`
/// reads it: `&` and `\g{0}` are the match, `\N`/`\gN`/`\g{N}` group N, a backslash
/// before anything else that character (`\0` is `0`), `$` nothing special. Values from
/// EMQX 6.3.1.
#[test]
fn regex_replace_reads_its_replacement_as_erlang_re_does() {
    for (rep, want) in [
        (r"\0", "a0c"),
        (r"\g{0}", "abc"),
        ("&", "abc"),
        (r"[\1]", "a[b]c"),
        (r"\g1", "abc"),
        (r"\g{1}0", "ab0c"),
        (r"\&", "a&c"),
        (r"\\", r"a\\c"),
        (r"\n", "anc"),
        (r"\", r"a\\c"),
        (r"\g", "agc"),
        (r"\9", "ac"),
        (r"\10", "ac"),
        ("$1", "a$1c"),
    ] {
        assert_eq!(
            val(&format!("regex_replace('abc', '(b)', '{rep}')")),
            format!("\"{want}\""),
            "replacement {rep}"
        );
    }
    refused(r"regex_replace('abc', '(b)', '\gx')");
    refused(r"regex_replace('abc', '(b)', '\g{x}')");
}

/// What EMQX 6.3.1 (OTP 28, PCRE2 10.47) gives for one subject and pattern:
/// `regex_match`, `regex_replace` with `<&>`, and `regex_extract`; `Err` where the call
/// raises (`badarg`), which fails the rule.
type RegexRow = (
    &'static [u8],
    &'static [u8],
    Result<bool, ()>,
    Result<&'static [u8], ()>,
    Result<&'static [&'static [u8]], ()>,
);

/// What this engine gives for the calls of a [`RegexRow`].
type RegexResults = (
    Result<bool, ()>,
    Result<Vec<u8>, ()>,
    Result<Vec<Vec<u8>>, ()>,
);

/// `regex_match`, `regex_replace(S, P, '<&>')` and `regex_extract` of `subject` and
/// `pattern` as values, called with the pattern compiled per call (as from a payload)
/// or, with `literal`, compiled once beforehand (as a literal in the rule is when the
/// file loads).
fn regex_calls(subject: &[u8], pattern: &[u8], literal: bool) -> RegexResults {
    let payload = Bytes::new();
    let props = mqtt_core::AppProperties::default();
    let input = msg("t/a", &payload, &props);
    let ctx = EvalCtx::new(&input);
    let compiled = funcs::compile_regex(pattern);
    let call = |name: &str, args: &[Value]| {
        let func = funcs::lookup(name).unwrap();
        let regex = literal.then_some(&compiled);
        (func.f)(args, &funcs::FnCtx { ctx: &ctx, regex }).map_err(|_| ())
    };
    let bytes = |b: &[u8]| Value::from_bytes(&Bytes::copy_from_slice(b));
    let (subject, pattern) = (bytes(subject), bytes(pattern));
    let matched = call("regex_match", &[subject.clone(), pattern.clone()])
        .map(|v| matches!(v, Value::Bool(true)));
    let replaced = call(
        "regex_replace",
        &[subject.clone(), pattern.clone(), Value::from("<&>")],
    )
    .map(|v| v.as_bytes().unwrap().to_vec());
    let extracted = call("regex_extract", &[subject, pattern]).map(|v| match v {
        Value::Array(groups) => groups
            .iter()
            .map(|g| g.as_bytes().unwrap().to_vec())
            .collect(),
        v => panic!("{v:?}"),
    });
    (matched, replaced, extracted)
}

/// EMQX's regex functions run Erlang's `re`: PCRE2 on bytes, not characters. The
/// probes that told the engines apart, each with EMQX 6.3.1's answer.
#[test]
fn regular_expressions_are_pcre2_on_bytes_as_in_emqx() {
    let on = |s: &str, sql_expr: &str| -> String {
        let out = one(
            &format!("SELECT {sql_expr} AS r FROM \"t/#\""),
            &format!(r#"{{"s":{}}}"#, serde_json::to_string(s).unwrap()),
        );
        let Value::Map(m) = json_decode(out.as_bytes()).unwrap() else {
            panic!("{out}")
        };
        m.get("r").unwrap().to_json().unwrap()
    };
    // `$` matches before a final newline.
    assert_eq!(on("abc\n", "regex_match(payload.s, 'abc$')"), "true");
    // `.` is one byte: `é` is two.
    assert_eq!(on("é", "regex_replace(payload.s, '.', 'x')"), r#""xx""#);
    assert_eq!(on("é", "regex_match(payload.s, '^.$')"), "false");
    // `\d` and `\w` are ASCII.
    assert_eq!(on("٣", "regex_match(payload.s, '^\\d$')"), "false");
    assert_eq!(on("é", "regex_match(payload.s, '\\w')"), "false");
    // Backreferences, look-around and possessive quantifiers compile.
    assert_eq!(on("aa", "regex_match(payload.s, '(a)\\1')"), "true");
    assert_eq!(
        on("foobar", "regex_extract(payload.s, 'foo(?=(bar))')"),
        r#"["bar"]"#
    );
    assert_eq!(on("aaa", "regex_match(payload.s, '^a++a')"), "false");
}

/// A broad differential set: every row is EMQX 6.3.1's own output for the same call
/// (`emqx_rule_funcs:regex_match/2`, `regex_replace/3`, `regex_extract/2`), run on the
/// EMQX container — line ends and newline conventions, bytes against characters,
/// PCRE2-only syntax, OTP's global-match loop around empty matches, the match limit as
/// no match, invalid patterns as `badarg`, and patterns that are not UTF-8. Each row is
/// checked with the pattern compiled per call and compiled once beforehand.
#[test]
fn regex_functions_match_emqx_row_for_row() {
    let mut differ = Vec::new();
    for (s, p, m, r, x) in EMQX_REGEX_ROWS {
        let want = (
            *m,
            r.map(<[u8]>::to_vec),
            x.map(|g| g.iter().map(|b| b.to_vec()).collect()),
        );
        for literal in [false, true] {
            let got = regex_calls(s, p, literal);
            if got != want {
                differ.push(format!(
                    "{:?} on {:?} (literal: {literal}): got {got:?}, EMQX {want:?}",
                    String::from_utf8_lossy(p),
                    String::from_utf8_lossy(s),
                ));
            }
        }
    }
    assert!(differ.is_empty(), "{}", differ.join("\n"));
}

/// OTP's `re` runs PCRE2 with its build's match and depth limits (10,000,000 each, per
/// start position) and reports reaching one as no match. `(a+)+b|z` on `a…az` tries
/// `(a+)+b` from the first `a` in time exponential in the run: EMQX finds the `z` after
/// 21 `a`s and gives up after 22. The same threshold here pins the same limits.
#[test]
fn the_match_limit_is_otps_and_reaching_it_is_no_match() {
    let at = |n: usize| regex_calls(format!("{}z", "a".repeat(n)).as_bytes(), b"(a+)+b|z", false).0;
    assert_eq!(at(21), Ok(true));
    assert_eq!(at(22), Ok(false));
    // A limit the pattern sets lower applies; reaching it is no match too (EMQX's
    // answers).
    let heap1 = b"(*LIMIT_HEAP=1)^(?:a|b)*$";
    assert_eq!(regex_calls(b"ab", heap1, false).0, Ok(true));
    assert_eq!(
        regex_calls("ab".repeat(50).as_bytes(), heap1, false).0,
        Ok(false)
    );
}

/// Catastrophic backtracking stops at the match limit, so a hostile pattern from a
/// payload costs one bounded match: the call is no match, as in EMQX — not an error —
/// and the rule, the message and the next message carry on.
#[test]
fn catastrophic_backtracking_is_bounded_and_no_match() {
    let sql = "SELECT regex_match(payload.s, payload.p) AS m, regex_replace(payload.s, payload.p, 'X') AS r, regex_extract(payload.s, payload.p) AS x FROM \"t/#\"";
    let hostile = format!(r#"{{"s":"{}b","p":"^(a+)+$"}}"#, "a".repeat(64));
    let started = std::time::Instant::now();
    assert_eq!(
        one(sql, &hostile),
        format!(r#"{{"m":false,"r":"{}b","x":[]}}"#, "a".repeat(64))
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
    assert_eq!(
        one(sql, r#"{"s":"aaa","p":"^(a+)+$"}"#),
        r#"{"m":true,"r":"X","x":["aaa"]}"#
    );
}

/// The heap a match may take for its backtracking frames is bounded here, where OTP's
/// 20,000,000 KiB is in effect no bound: `(?:a|b)*` over a long subject needs a frame per
/// character. Past [`funcs::re::HEAP_LIMIT_KIB`] the match is no match — EMQX matches this
/// 1 MB subject — and a pattern cannot raise the bound with its own `(*LIMIT_HEAP=…)`.
#[test]
fn a_matchs_backtracking_heap_is_bounded() {
    let long = "ab".repeat(500_000);
    let short = "ab".repeat(1_000);
    assert_eq!(
        regex_calls(short.as_bytes(), b"^(?:a|b)*$", false).0,
        Ok(true)
    );
    assert_eq!(
        regex_calls(long.as_bytes(), b"^(?:a|b)*$", false).0,
        Ok(false)
    );
    assert_eq!(
        regex_calls(long.as_bytes(), b"(*LIMIT_HEAP=100000000)^(?:a|b)*$", false).0,
        Ok(false)
    );
    // Start-of-pattern items still read as the pattern's own, before the bound.
    assert_eq!(
        regex_calls(b"a\r\nb", b"(*CRLF)(*LIMIT_MATCH=100)(?m)a$", false).0,
        Ok(true)
    );
}

/// A literal pattern that does not compile no longer fails the load: EMQX accepts the
/// rule (its parser does not compile patterns) and every call raises `badarg`. The load
/// says so as a warning, and each message fails the rule with PCRE2's message.
#[test]
fn an_invalid_pattern_loads_with_a_warning_and_fails_each_call() {
    let sql = "SELECT regex_match(payload.s, '(') AS m FROM \"t/#\"";
    let warnings = check_sql(sql).unwrap();
    assert!(
        warnings.iter().any(|w| w.contains(
            "regex_match(): invalid regular expression: missing closing parenthesis at position 1"
        )),
        "{warnings:?}"
    );
    let e = fails(sql, r#"{"s":"abc"}"#);
    assert!(e.contains("missing closing parenthesis"), "{e}");
    // The same from a payload.
    let e = fails(
        "SELECT regex_match(payload.s, payload.p) AS m FROM \"t/#\"",
        r#"{"s":"abc","p":"a{2,1}"}"#,
    );
    assert!(e.contains("numbers out of order in {} quantifier"), "{e}");
    // Positions are in the pattern as written (EMQX: `{"length of lookbehind assertion is not limited",6}`).
    let e = funcs::compile_regex(b"(*UTF)(?<=a+)b").unwrap_err();
    assert!(e.ends_with("at position 6"), "{e}");
}

/// A pattern that is not UTF-8 (from a binary payload) still means its own bytes:
/// written for the `pcre2` crate's `&str` as `\xHH` escapes, closing and reopening a
/// `\Q…\E` around them and keeping an escaping backslash.
#[test]
fn a_pattern_that_is_not_utf8_means_its_own_bytes() {
    for (pattern, subject, want) in [
        (&b"\xe9"[..], &b"\xe9"[..], true),
        (b"\\\xe9", b"\xe9", true),
        (b"\\Q.\xe9\\E", b".\xe9", true),
        (b"\\Q.\xe9\\E", b"x\xe9", false),
        (b"\\Q\\\xe9\\E", b"\\\xe9", true),
        (b"\\Q\\\\E\xe9", b"\\\xe9", true),
        (b"[\xe0-\xef]", b"\xe9", true),
        (b"(?#\xff)a", b"a", true),
    ] {
        assert_eq!(
            regex_calls(subject, pattern, false).0,
            Ok(want),
            "{pattern:?} on {subject:?}"
        );
    }
}

const EMQX_REGEX_ROWS: &[RegexRow] = &[
    (b"abc\n", b"abc$", Ok(true), Ok(b"<abc>\n"), Ok(&[])),
    (b"abc\n", b"abc\\z", Ok(false), Ok(b"abc\n"), Ok(&[])),
    (b"abc\n", b"abc\\Z", Ok(true), Ok(b"<abc>\n"), Ok(&[])),
    (b"abc\n\n", b"abc$", Ok(false), Ok(b"abc\n\n"), Ok(&[])),
    (b"abc\n", b"(?m)abc$", Ok(true), Ok(b"<abc>\n"), Ok(&[])),
    (b"a\nb", b"^b", Ok(false), Ok(b"a\nb"), Ok(&[])),
    (b"a\nb", b"(?m)^b", Ok(true), Ok(b"a\n<b>"), Ok(&[])),
    (b"a\nb", b"a.b", Ok(false), Ok(b"a\nb"), Ok(&[])),
    (b"a\nb", b"(?s)a.b", Ok(true), Ok(b"<a\nb>"), Ok(&[])),
    (b"a\r\nb", b"a$", Ok(false), Ok(b"a\r\nb"), Ok(&[])),
    (b"a\r\nb", b"(?m)a$", Ok(false), Ok(b"a\r\nb"), Ok(&[])),
    (b"a\rb", b"(?m)a$", Ok(false), Ok(b"a\rb"), Ok(&[])),
    (b"a\r\nb", b"a\\Rb", Ok(true), Ok(b"<a\r\nb>"), Ok(&[])),
    (b"a\rb", b"(*CR)(?m)a$", Ok(true), Ok(b"<a>\rb"), Ok(&[])),
    (
        b"a\r\nb",
        b"(*CRLF)(?m)a$",
        Ok(true),
        Ok(b"<a>\r\nb"),
        Ok(&[]),
    ),
    (
        b"a\r\nb",
        b"(*ANYCRLF)(?m)^b",
        Ok(true),
        Ok(b"a\r\n<b>"),
        Ok(&[]),
    ),
    (
        b"a\x85b",
        b"(*ANY)(?m)a$",
        Ok(true),
        Ok(b"<a>\x85b"),
        Ok(&[]),
    ),
    (b"\n", b"^$", Ok(true), Ok(b"<>\n"), Ok(&[])),
    (b"\n\n", b"(?m)^$", Ok(true), Ok(b"<>\n<>\n"), Ok(&[])),
    (
        b"a\r\nb\r\n",
        b"(*CRLF)(?m)$",
        Ok(true),
        Ok(b"a<>\r\nb<>\r\n<>"),
        Ok(&[]),
    ),
    (
        b"a\r\nb",
        b"(*CRLF)",
        Ok(true),
        Ok(b"<>a<>\r\n<>b<>"),
        Ok(&[]),
    ),
    (
        b"a\r\nb",
        b"(*LF)x*",
        Ok(true),
        Ok(b"<>a<>\r<>\n<>b<>"),
        Ok(&[]),
    ),
    (b"\xc3\xa9", b"^.$", Ok(false), Ok(b"\xc3\xa9"), Ok(&[])),
    (b"\xc3\xa9", b"^..$", Ok(true), Ok(b"<\xc3\xa9>"), Ok(&[])),
    (b"\xc3\xa9", b"\\w", Ok(false), Ok(b"\xc3\xa9"), Ok(&[])),
    (
        b"\xc3\xa9",
        b"^\\W\\W$",
        Ok(true),
        Ok(b"<\xc3\xa9>"),
        Ok(&[]),
    ),
    (b"\xd9\xa3", b"^\\d$", Ok(false), Ok(b"\xd9\xa3"), Ok(&[])),
    (b"\xd9\xa3", b"\\d", Ok(false), Ok(b"\xd9\xa3"), Ok(&[])),
    (
        b"\xc3\x89",
        b"(?i)\xc3\xa9",
        Ok(false),
        Ok(b"\xc3\x89"),
        Ok(&[]),
    ),
    (
        b"\xc3\xa9",
        b"(.)",
        Ok(true),
        Ok(b"<\xc3><\xa9>"),
        Ok(&[b"\xc3"]),
    ),
    (b"\xc3\xa9", b".", Ok(true), Ok(b"<\xc3><\xa9>"), Ok(&[])),
    (
        b"\xc3\xa9a",
        b"\\W",
        Ok(true),
        Ok(b"<\xc3><\xa9>a"),
        Ok(&[]),
    ),
    (
        b"\xe2\x82\xac",
        b"[^a]",
        Ok(true),
        Ok(b"<\xe2><\x82><\xac>"),
        Ok(&[]),
    ),
    (
        b"\xc3\xbc",
        b"\\xc3\\xbc",
        Ok(true),
        Ok(b"<\xc3\xbc>"),
        Ok(&[]),
    ),
    (b"\xc3\xa9", b"\\x{e9}", Ok(false), Ok(b"\xc3\xa9"), Ok(&[])),
    (
        b"\xc3\xa9",
        b"[\\x80-\\xff]+",
        Ok(true),
        Ok(b"<\xc3\xa9>"),
        Ok(&[]),
    ),
    (b"\xc3\xa9", b"\\p{L}", Ok(true), Ok(b"<\xc3>\xa9"), Ok(&[])),
    (
        b"\xc3\xa9",
        b"[[:alpha:]]",
        Ok(false),
        Ok(b"\xc3\xa9"),
        Ok(&[]),
    ),
    (
        b"\xc3\xa9",
        b"[[:^ascii:]]+",
        Ok(true),
        Ok(b"<\xc3\xa9>"),
        Ok(&[]),
    ),
    (
        b"Stra\xc3\x9fe",
        b"(?i)STRASSE",
        Ok(false),
        Ok(b"Stra\xc3\x9fe"),
        Ok(&[]),
    ),
    (
        b"\xc3\x80B",
        b"(?i)\xc3\xa0b",
        Ok(false),
        Ok(b"\xc3\x80B"),
        Ok(&[]),
    ),
    (b"ABC", b"(?i)abc", Ok(true), Ok(b"<ABC>"), Ok(&[])),
    (
        b"\xc3\xa9",
        b"(*UTF)^.$",
        Ok(true),
        Ok(b"<\xc3\xa9>"),
        Ok(&[]),
    ),
    (
        b"\xc3\xa9",
        b"(*UTF)(*UCP)^\\w$",
        Ok(true),
        Ok(b"<\xc3\xa9>"),
        Ok(&[]),
    ),
    (
        b"\xc3\xa9",
        b"(*UCP)\\w",
        Ok(true),
        Ok(b"<\xc3>\xa9"),
        Ok(&[]),
    ),
    (
        b"x\xc2\xa0y",
        b"x\\sy",
        Ok(false),
        Ok(b"x\xc2\xa0y"),
        Ok(&[]),
    ),
    (b"a\x0bb", b"a\\sb", Ok(true), Ok(b"<a\x0bb>"), Ok(&[])),
    (b"a\x0cb", b"a\\sb", Ok(true), Ok(b"<a\x0cb>"), Ok(&[])),
    (b"a_1", b"^\\w+$", Ok(true), Ok(b"<a_1>"), Ok(&[])),
    (b"\t", b"\\h", Ok(true), Ok(b"<\t>"), Ok(&[])),
    (b"\x0b", b"\\v", Ok(true), Ok(b"<\x0b>"), Ok(&[])),
    (b"a b", b"\\bb", Ok(true), Ok(b"a <b>"), Ok(&[])),
    (b"\xc3\xa9a", b"\\ba", Ok(true), Ok(b"\xc3\xa9<a>"), Ok(&[])),
    (b"aa", b"(a)\\1", Ok(true), Ok(b"<aa>"), Ok(&[b"a"])),
    (b"ab", b"(a)\\1", Ok(false), Ok(b"ab"), Ok(&[])),
    (
        b"abab",
        b"(?<x>ab)\\k<x>",
        Ok(true),
        Ok(b"<abab>"),
        Ok(&[b"ab"]),
    ),
    (
        b"abcabc",
        b"(abc)\\g1",
        Ok(true),
        Ok(b"<abcabc>"),
        Ok(&[b"abc"]),
    ),
    (
        b"abab",
        b"(ab)\\g{-1}",
        Ok(true),
        Ok(b"<abab>"),
        Ok(&[b"ab"]),
    ),
    (b"foobar", b"foo(?=bar)", Ok(true), Ok(b"<foo>bar"), Ok(&[])),
    (b"foobaz", b"foo(?=bar)", Ok(false), Ok(b"foobaz"), Ok(&[])),
    (b"foobaz", b"foo(?!bar)", Ok(true), Ok(b"<foo>baz"), Ok(&[])),
    (b"xbar", b"(?<=x)bar", Ok(true), Ok(b"x<bar>"), Ok(&[])),
    (b"ybar", b"(?<!x)bar", Ok(true), Ok(b"y<bar>"), Ok(&[])),
    (b"aaa", b"^a++a", Ok(false), Ok(b"aaa"), Ok(&[])),
    (b"aaa", b"^(?>a+)a", Ok(false), Ok(b"aaa"), Ok(&[])),
    (b"aaa", b"^a*+$", Ok(true), Ok(b"<aaa>"), Ok(&[])),
    (
        b"((()))",
        b"^(\\((?1)*\\))$",
        Ok(true),
        Ok(b"<((()))>"),
        Ok(&[b"((()))"]),
    ),
    (b"(()", b"^(\\((?1)*\\))$", Ok(false), Ok(b"(()"), Ok(&[])),
    (
        b"abba",
        b"^((.)(?1)\\2|.?)$",
        Ok(true),
        Ok(b"<abba>"),
        Ok(&[b"abba", b"a"]),
    ),
    (
        b"ab",
        b"^(a)?(?(1)b|c)$",
        Ok(true),
        Ok(b"<ab>"),
        Ok(&[b"a"]),
    ),
    (b"c", b"^(a)?(?(1)b|c)$", Ok(true), Ok(b"<c>"), Ok(&[])),
    (b"xyz", b"(?|(x)|(y))z", Ok(true), Ok(b"x<yz>"), Ok(&[b"y"])),
    (b"abc", b"(?<=\\Ga)b", Ok(true), Ok(b"a<b>c"), Ok(&[])),
    (
        b"abc",
        b"a(*SKIP)(*FAIL)|c",
        Ok(true),
        Ok(b"ab<c>"),
        Ok(&[]),
    ),
    (b"abc", b"(*COMMIT)b", Ok(true), Ok(b"a<b>c"), Ok(&[])),
    (b"abc", b"a(*ACCEPT)x", Ok(true), Ok(b"<a>bc"), Ok(&[])),
    (b"aaab", b"a+(*PRUNE)b", Ok(true), Ok(b"<aaab>"), Ok(&[])),
    (b"abc", b"(?C1)abc", Ok(true), Ok(b"<abc>"), Ok(&[])),
    (
        b"abc",
        b"(*LIMIT_MATCH=10)a",
        Ok(true),
        Ok(b"<a>bc"),
        Ok(&[]),
    ),
    (b"abc", b"(*NOTEMPTY)x*", Ok(false), Ok(b"abc"), Ok(&[])),
    (b"abc", b"(*NO_START_OPT)b", Ok(true), Ok(b"a<b>c"), Ok(&[])),
    (b"abc", b"[[:alpha:]]+", Ok(true), Ok(b"<abc>"), Ok(&[])),
    (b"a.c", b"a\\.c", Ok(true), Ok(b"<a.c>"), Ok(&[])),
    (b"abc", b"a\\Qb\\Ec", Ok(true), Ok(b"<abc>"), Ok(&[])),
    (b"a.c", b"\\Qa.c\\E", Ok(true), Ok(b"<a.c>"), Ok(&[])),
    (b"ab", b"(?x) a b # comment", Ok(true), Ok(b"<ab>"), Ok(&[])),
    (b"ab", b"a(?#comment)b", Ok(true), Ok(b"<ab>"), Ok(&[])),
    (b"aXb", b"a\\Cb", Ok(true), Ok(b"<aXb>"), Ok(&[])),
    (b"abc", b"\\Aabc\\z", Ok(true), Ok(b"<abc>"), Ok(&[])),
    (b"x\x00y", b"x\\x00y", Ok(true), Ok(b"<x\x00y>"), Ok(&[])),
    (b"x\x00y", b"x.y", Ok(true), Ok(b"<x\x00y>"), Ok(&[])),
    (b"x\x00y", b"x\\0y", Ok(true), Ok(b"<x\x00y>"), Ok(&[])),
    (b"aaa", b"a{2}", Ok(true), Ok(b"<aa>a"), Ok(&[])),
    (b"aaa", b"^a{,2}", Ok(true), Ok(b"<aa>a"), Ok(&[])),
    (b"a{,2}", b"^a{,2}$", Ok(false), Ok(b"a{,2}"), Ok(&[])),
    (b"abc", b"\\N", Ok(true), Ok(b"<a><b><c>"), Ok(&[])),
    (b"a\nb", b"a\\Nb", Ok(false), Ok(b"a\nb"), Ok(&[])),
    (b"ab12", b"(?i:A)B\\d+", Ok(false), Ok(b"ab12"), Ok(&[])),
    (b"abc", b"[^\\w]", Ok(false), Ok(b"abc"), Ok(&[])),
    (b"a-z", b"[a\\-z]+", Ok(true), Ok(b"<a-z>"), Ok(&[])),
    (b"]", b"[]]", Ok(true), Ok(b"<]>"), Ok(&[])),
    (b"abc", b"", Ok(true), Ok(b"<>a<>b<>c<>"), Ok(&[])),
    (b"", b"", Ok(true), Ok(b"<>"), Ok(&[])),
    (b"", b"^$", Ok(true), Ok(b"<>"), Ok(&[])),
    (b"", b"a*", Ok(true), Ok(b"<>"), Ok(&[])),
    (b"abc", b"x*", Ok(true), Ok(b"<>a<>b<>c<>"), Ok(&[])),
    (b"aaa", b"a*", Ok(true), Ok(b"<aaa><>"), Ok(&[])),
    (b"aaa", b"a*?", Ok(true), Ok(b"<><a><><a><><a><>"), Ok(&[])),
    (b"abc", b"(?=a)", Ok(true), Ok(b"<>abc"), Ok(&[])),
    (b"abc", b"\\b", Ok(true), Ok(b"<>abc<>"), Ok(&[])),
    (
        b"hello world",
        b"(\\w+) (\\w+)",
        Ok(true),
        Ok(b"<hello world>"),
        Ok(&[b"hello", b"world"]),
    ),
    (b"abc", b"(x)?b", Ok(true), Ok(b"a<b>c"), Ok(&[])),
    (b"abc", b"(x)?(b)", Ok(true), Ok(b"a<b>c"), Ok(&[b"", b"b"])),
    (b"abc", b"(b)(x)?", Ok(true), Ok(b"a<b>c"), Ok(&[b"b"])),
    (
        b"abc",
        b"(b)(x)?(c)",
        Ok(true),
        Ok(b"a<bc>"),
        Ok(&[b"b", b"", b"c"]),
    ),
    (b"abc", b"b", Ok(true), Ok(b"a<b>c"), Ok(&[])),
    (b"abc", b"(?<n>b)", Ok(true), Ok(b"a<b>c"), Ok(&[b"b"])),
    (b"abcabc", b"(b)", Ok(true), Ok(b"a<b>ca<b>c"), Ok(&[b"b"])),
    (b"abc", b"()", Ok(true), Ok(b"<>a<>b<>c<>"), Ok(&[b""])),
    (b"abc", b"\\Kb", Ok(true), Ok(b"a<b>c"), Ok(&[])),
    (b"abc", b"a\\K", Ok(true), Ok(b"a<>bc"), Ok(&[])),
    (b"abc", b"a\\K(b)", Ok(true), Ok(b"a<b>c"), Ok(&[b"b"])),
    (
        b"aaa",
        b"(?=a)|a",
        Ok(true),
        Ok(b"<><a><><a><><a>"),
        Ok(&[]),
    ),
    (b"abcabc", b"(?<=a)b", Ok(true), Ok(b"a<b>ca<b>c"), Ok(&[])),
    (
        b"1,22,,333",
        b"\\d*",
        Ok(true),
        Ok(b"<1><>,<22><>,<>,<333><>"),
        Ok(&[]),
    ),
    (
        b"one two  three",
        b"\\s*",
        Ok(true),
        Ok(b"<>o<>n<>e< ><>t<>w<>o<  ><>t<>h<>r<>e<>e<>"),
        Ok(&[]),
    ),
    (b"ab", b"(?=b)|b", Ok(true), Ok(b"a<><b>"), Ok(&[])),
    (b"ab", b"\\B", Ok(true), Ok(b"a<>b"), Ok(&[])),
    (b"a.b.c", b"\\.", Ok(true), Ok(b"a<.>b<.>c"), Ok(&[])),
    (
        b"192.168.0.1",
        b"^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$",
        Ok(true),
        Ok(b"<192.168.0.1>"),
        Ok(&[b"192", b"168", b"0", b"1"]),
    ),
    (
        b"temp=21.5C",
        b"temp=(?<v>[0-9.]+)(?<u>[CF])",
        Ok(true),
        Ok(b"<temp=21.5C>"),
        Ok(&[b"21.5", b"C"]),
    ),
    (
        b"Date: 2021-05-20",
        b"(\\d{4})-(\\d{2})-(\\d{2})",
        Ok(true),
        Ok(b"Date: <2021-05-20>"),
        Ok(&[b"2021", b"05", b"20"]),
    ),
    (
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        b"^(a+)+$",
        Ok(false),
        Ok(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"),
        Ok(&[]),
    ),
    (
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        b"^(a+)+$",
        Ok(true),
        Ok(b"<aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa>"),
        Ok(&[b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]),
    ),
    (
        b"xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        b"x|^(a+)+$",
        Ok(true),
        Ok(b"<x>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"),
        Ok(&[]),
    ),
    (
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        b"(a+)+$|b",
        Ok(false),
        Ok(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"),
        Ok(&[]),
    ),
    (
        b"xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        b"x|(a+)+$",
        Ok(true),
        Ok(b"<x>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"),
        Ok(&[]),
    ),
    (
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!",
        b"(a|aa)+$",
        Ok(false),
        Ok(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!"),
        Ok(&[]),
    ),
    (
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!",
        b"(?:a|a)*$",
        Ok(false),
        Ok(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!"),
        Ok(&[]),
    ),
    (
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        b"(*LIMIT_MATCH=1000)^(a+)+b",
        Ok(true),
        Ok(b"<aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab>"),
        Ok(&[b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]),
    ),
    (b"abc", b"(", Err(()), Err(()), Err(())),
    (b"abc", b"[a", Err(()), Err(()), Err(())),
    (b"abc", b"a{2,1}", Err(()), Err(()), Err(())),
    (b"abc", b"\\", Err(()), Err(()), Err(())),
    (b"abc", b"(?<=a+)b", Err(()), Err(()), Err(())),
    (b"abc", b"\\p{Foo}", Err(()), Err(()), Err(())),
    (b"abc", b"x{70000}", Err(()), Err(()), Err(())),
    (b"abc", b"\\k<zz>", Err(()), Err(()), Err(())),
    (b"abc", b"*", Err(()), Err(()), Err(())),
    (b"abc", b"\\8", Err(()), Err(()), Err(())),
    (b"abc", b"(?P<1>a)", Err(()), Err(()), Err(())),
    (b"abc", b"(*FOO)a", Err(()), Err(()), Err(())),
    (b"abc", b"(*LIMIT_HEAP=x)a", Err(()), Err(()), Err(())),
    (b"abc", b"a)", Err(()), Err(()), Err(())),
    (b"abc", b"(?z)", Err(()), Err(()), Err(())),
    (b"abc", b"\\c", Err(()), Err(()), Err(())),
    (b"abc", b"[z-a]", Err(()), Err(()), Err(())),
    (b"abc", b"(?<=a(?1))(b)", Ok(false), Ok(b"abc"), Ok(&[])),
    (b"abc", b"(?(?{1})a)", Err(()), Err(()), Err(())),
    (b"abc", b"\\g", Err(()), Err(()), Err(())),
    (b"abc", b"a**", Err(()), Err(()), Err(())),
    (b"\xe9", b"(*UTF).", Err(()), Err(()), Err(())),
    (b"\xff\xfe", b"(*UTF)x", Err(()), Err(()), Err(())),
    (b"x\xffy", b"x\\xffy", Ok(true), Ok(b"<x\xffy>"), Ok(&[])),
    (b"x\xffy", b"x.y", Ok(true), Ok(b"<x\xffy>"), Ok(&[])),
    (b"x\xe9", b"x\xe9", Ok(true), Ok(b"<x\xe9>"), Ok(&[])),
    (b"\xe9", b"^\xe9$", Ok(true), Ok(b"<\xe9>"), Ok(&[])),
    (b"a\xe9b", b"[\xe9]", Ok(true), Ok(b"a<\xe9>b"), Ok(&[])),
    (b"a\xe9b", b"\\Q\xe9\\E", Ok(true), Ok(b"a<\xe9>b"), Ok(&[])),
    (b"a\xe9b", b"\\\xe9", Ok(true), Ok(b"a<\xe9>b"), Ok(&[])),
    (
        b"\\\xe9",
        b"\\Q\\\xe9\\E",
        Ok(true),
        Ok(b"<\\\xe9>"),
        Ok(&[]),
    ),
];

/// EMQX's lexer keeps a quoted token whole and its parser unquotes it with
/// `string:trim(Text, both, "'")`: a doubled quote inside stays doubled and every quote
/// at either end goes. Values from EMQX 6.3.1.
#[test]
fn doubled_quotes_in_a_literal_stay_doubled_and_edge_quotes_go() {
    assert_eq!(val("'it''s'"), r#""it''s""#);
    assert_eq!(val("'''x'''"), r#""x""#);
    assert_eq!(val("''''"), r#""""#);
    assert_eq!(val("'a''''b'"), r#""a''''b""#);
    assert_eq!(
        one(
            r#"SELECT payload."a""b" AS r FROM "t/#""#,
            r#"{"a\"\"b":1}"#
        ),
        r#"{"r":1}"#
    );
}

/// `sprintf` is `io_lib:format/2` on the values as EMQX holds them (strings are
/// binaries, arrays lists, `true`/`null` atoms), with the format read as bytes and the
/// output required to fit in bytes. Every value is EMQX 6.3.1's.
#[test]
fn sprintf_is_erlang_io_lib_format() {
    let cases: &[(&str, &str)] = &[
        ("sprintf('~s|~s|~s', 'abc', json_decode('[104,105]'), json_decode('[\"a\",[\"b\"]]'))", r#""abc|hi|ab""#),
        ("sprintf('~s', true)", r#""true""#),
        ("sprintf('é~s', 'é')", r#""éé""#),
        ("bin2hexstr(sprintf('~ts', 'é'))", r#""E9""#),
        ("sprintf('~p|~w', 'abc', 'abc')", r#""<<\"abc\">>|<<97,98,99>>""#),
        ("sprintf('~p', 'é')", r#""<<\"é\">>""#),
        ("bin2hexstr(sprintf('~tp', 'é'))", r#""3C3C22E9222F757466383E3E""#),
        (r#"sprintf('~p', unescape('a\nb"c\\d\te'))"#, r#""<<\"a\\nb\\\"c\\\\d\\te\">>""#),
        ("sprintf('~p|~w', json_decode('{\"b\":1,\"a\":[1,2]}'), json_decode('{\"b\":1,\"a\":[1,2]}'))", r##""#{<<\"a\">> => [1,2],<<\"b\">> => 1}|#{<<97>> => [1,2],<<98>> => 1}""##),
        ("sprintf('~p|~w', json_decode('[104,105]'), json_decode('[104,105]'))", r#""\"hi\"|[104,105]""#),
        ("sprintf('~p', json_decode('[\"x\",1.5,true,null,-3]'))", r#""[<<\"x\">>,1.5,true,null,-3]""#),
        ("sprintf('~p', payload.nope)", r#""undefined""#),
        ("sprintf('~p|~p|~p|~p|~p|~p|~p', 1.0, 0.1, 100000.0, 1.0e15, 1.0e16, 9007199254740992.0, 9007199254740991.0)", r#""1.0|0.1|1.0e5|1.0e15|1.0e16|9.007199254740992e15|9007199254740991.0""#),
        ("sprintf('~p|~p|~p|~p', 0.0001, 1000.0, 100.0, 12345678901234567.0)", r#""0.0001|1.0e3|100.0|1.2345678901234568e16""#),
        ("sprintf('~f|~.2f|~e|~g|~.3e|~10.3f|~-10.3f|~3.1f', 3.14159, 0.125, 3.14159, 3.14159, 1234.5, 3.14159, 3.14159, 1234.5)", r#""3.141590|0.13|3.14159e+0|3.14159|1.23e+3|     3.142|3.142     |***""#),
        ("sprintf('~g|~g|~g|~g|~.1g|~.2g', 0.05, 12345.0, 0.5, 100.0, 0.5, 0.05)", r#""5.00000e-2|1.23450e+4|0.500000|100.000|0.5|5.0e-2""#),
        ("sprintf('~.15f|~.20e|~f|~e', 0.1, 0.1, 999.9999999, 9.9999999)", r#""0.100000000000000|1.0000000000000000555e-1|1000.000000|1.00000e+1""#),
        ("sprintf('~b|~.16b|~.16B|~.2b|~-6b|~6b|~6.16.0B|~.36b', -42, 255, 255, 5, 7, 7, 255, 35)", r#""-42|ff|FF|101|7     |     7|0000FF|z""#),
        ("sprintf('~.16x|~.16X|~.16#|~.16+', 255, json_decode('[48,120]'), -255, json_decode('[48,120]'), 255, 255)", r#""0xff|-0xFF|16#FF|16#ff""#),
        ("sprintf('~c|~5c|~-3.2.xc|~c', 97, 98, 99, 321)", r#""a|bbbbb|ccx|A""#),
        ("sprintf('a~ib|~n|~3n|~~|~3~', 'ignored')", r#""ab|\n|\n\n\n|~|~~~""#),
        ("sprintf('~5s|~-5s|~.2s|~5.2s|~5.2.*s|~*s|~5.2.-s', 'abc', 'abc', 'abc', 'abc', 35, 'abc', 4, 'x', 'abc')", r#""  abc|abc  |ab|   ab|###ab|   x|---ab""#),
        ("sprintf('~*.*.*s|~-*s|~*s|', 6, 2, 46, 'abc', -6, 'abc', -6, 'abc')", r#""....ab|   abc|abc   |""#),
        ("sprintf('~5w|~-6w|~2w|~5p', 12, 12, 12345, 12)", r#""   12|12    |**|12""#),
        ("sprintf('~W|~P', json_decode('[1,2,3,4,5]'), 3, 'abcdefgh', 3)", r#""[1,2|...]|<<\"abcdefgh\">>""#),
        ("sprintf('~W|~P', json_decode('{\"a\":1}'), 2, json_decode('{\"a\":1}'), 2)", r##""#{<<...>> => 1,...}|#{<<...>> => 1}""##),
        ("sprintf('~P|~P', 'abcdefghijklmnopqrstuvwxyz', 3, json_decode('[[1,2,3],[4,5,6]]'), 3)", r#""<<\"abcdefgh\"...>>|[[1|...],[...]]""#),
        ("sprintf('~lp|~kp', json_decode('[104,105]'), json_decode('{\"b\":1,\"a\":2}'))", r#""[104,105]|#{<<\"a\">> => 2,<<\"b\">> => 1}""#),
        ("sprintf('~.2ts', unescape('\\r\\nab'))", r#""\r\na""#),
        ("sprintf('~10.4e|~-12.3g|~10.3.0f', 3.14159, 2.5, -1.5)", r#""  3.142e+0|2.50        |0000-1.500""#),
        ("sprintf_s('~p-~p', json_decode('[1,2]'))", r#""1-2""#),
    ];
    for (expr, want) in cases {
        assert_eq!(val(expr), *want, "{expr}");
    }
    for bad in [
        "sprintf('~s', 1)",
        "sprintf('~s', json_decode('{\"a\":1}'))",
        "sprintf('~ts', '€')",
        "sprintf('~tp', '€')",
        "sprintf('~f', 1)",
        "sprintf('~.0f', 1.5)",
        "sprintf('~.1e', 1.5)",
        "sprintf('~x', 255, '0x')",
        "sprintf('~.37b', 1)",
        "sprintf('~tc', 8364)",
        "sprintf('~-3n')",
        "sprintf('~Kp', 'x', json_decode('{}'))",
        "sprintf('~s')",
        "sprintf('x', 1)",
        "sprintf('~q', 1)",
        "sprintf('~')",
        "sprintf('~.*s', 'x', 'abc')",
        "sprintf_s('~p', 1)",
    ] {
        refused(bad);
    }
}

/// A field width or precision can come from the payload; the output it asks for is
/// refused against the message's growth budget before anything is allocated (a width
/// of 2^62 would otherwise abort the process).
#[test]
fn sprintf_widths_from_the_payload_are_bounded() {
    for sql in [
        "SELECT sprintf('~*s', payload.w, 'x') AS r FROM \"t/#\"",
        "SELECT sprintf('~*c', payload.w, 97) AS r FROM \"t/#\"",
        "SELECT sprintf('~.*f', payload.w, 1.5) AS r FROM \"t/#\"",
    ] {
        let err = run_on(sql, "t/a", r#"{"w":4611686018427387903}"#).unwrap_err();
        assert!(err.contains("too large"), "{sql}: {err}");
    }
}

/// The compression functions are Erlang's `zlib` (C zlib, level 6) and EMQX's liblz4
/// NIF, so the bytes are EMQX's own. Every value is EMQX 6.3.1's.
#[test]
fn compression_matches_emqx_byte_for_byte() {
    assert_eq!(
        val("bin2hexstr(gzip('hello'))"),
        r#""1F8B0800000000000003CB48CDC9C9070086A6103605000000""#
    );
    assert_eq!(val("bin2hexstr(zip('hello'))"), r#""CB48CDC9C90700""#);
    assert_eq!(
        val("bin2hexstr(zip_compress('hello'))"),
        r#""789CCB48CDC9C90700062C0215""#
    );
    // Longer inputs, where a different deflate or LZ4 match finder would differ: the
    // sentence 100 and 3000 times (`pad` repeats its whole pad string), and above 64 KiB
    // the LZ4 blocks are linked, as `LZ4F_compressFrame` links them.
    let fox = |n: u32| {
        format!("pad('', {n}, 'trailing', 'The quick brown fox jumps over the lazy dog. ')")
    };
    assert_eq!(
        val(&format!("md5(zip_compress({}))", fox(100))),
        r#""8a2ec17166cc873df7266dc330ea1c0d""#
    );
    assert_eq!(
        val(&format!("md5(gzip({}))", fox(3000))),
        r#""39066a19601c0cc6ffbdc1f4525afae8""#
    );
    assert_eq!(
        val(&format!("md5(zip({}))", fox(3000))),
        r#""3e9a26854526b09b147bf91d89c2ee93""#
    );
    assert_eq!(
        val(&format!("md5(lz4_compress({}))", fox(3000))),
        r#""831a77815c70c43e4dbbd00e9120daf5""#
    );
    assert_eq!(
        val("bin2hexstr(lz4_compress('hello hello hello'))"),
        r#""04224D186040821100008068656C6C6F2068656C6C6F2068656C6C6F00000000""#
    );
    assert_eq!(
        val("bin2hexstr(lz4_compress(''))"),
        r#""04224D1860408200000000""#
    );
    // Concatenated gzip members decode; anything else after a member fails it. zlib and
    // raw streams ignore what follows them.
    let hello_gz = "1F8B0800000000000003CB48CDC9C9070086A6103605000000";
    assert_eq!(
        val(&format!("gunzip(hexstr2bin('{hello_gz}{hello_gz}'))")),
        r#""hellohello""#
    );
    refused(&format!("gunzip(hexstr2bin('{hello_gz}00'))"));
    assert_eq!(val("unzip(hexstr2bin('CB48CDC9C9070000'))"), r#""hello""#);
    assert_eq!(
        val("zip_uncompress(hexstr2bin('789CCB48CDC9C90700062C021500'))"),
        r#""hello""#
    );
    // lz4_uncompress reads the first frame and ignores the rest.
    assert_eq!(
        val("lz4_uncompress(hexstr2bin('04224D186040821100008068656C6C6F2068656C6C6F2068656C6C6F00000000FF'))"),
        r#""hello hello hello""#
    );
    for bad in [
        "gunzip('x')",
        "zip_uncompress(hexstr2bin('789CCB48'))",
        "unzip('')",
        "zip_uncompress('')",
        "gunzip('')",
        "lz4_uncompress('hello')",
        "lz4_uncompress(hexstr2bin('04224D1860408205000080'))",
    ] {
        refused(bad);
    }
}

/// A few hundred payload bytes can inflate to gigabytes: decompression stops at the
/// message's growth budget, while a payload within it decompresses.
#[test]
fn decompression_is_bounded_against_bombs() {
    // 8 MiB of zeros, deflated to about 8 KiB; the payload carries the compressed bytes.
    let zeros = vec![0u8; 8 << 20];
    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(9));
    std::io::Write::write_all(&mut e, &zeros).unwrap();
    let bomb = mqtt_core::hex_lower(&e.finish().unwrap());
    assert!(bomb.len() < 64 * 1024);
    let payload = format!(r#"{{"z":"{bomb}"}}"#);
    let err = run_on(
        "SELECT bytesize(zip_uncompress(hexstr2bin(payload.z))) AS r FROM \"t/#\"",
        "t/a",
        &payload,
    )
    .unwrap_err();
    assert!(err.contains("decompression stopped"), "{err}");
    let mut lz = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
    std::io::Write::write_all(&mut lz, &zeros).unwrap();
    let (lz, r) = lz.finish();
    r.unwrap();
    let payload = format!(r#"{{"z":"{}"}}"#, mqtt_core::hex_lower(&lz));
    let err = run_on(
        "SELECT bytesize(lz4_uncompress(hexstr2bin(payload.z))) AS r FROM \"t/#\"",
        "t/a",
        &payload,
    )
    .unwrap_err();
    assert!(err.contains("decompression stopped"), "{err}");
    // 512 KiB fits the budget.
    assert_eq!(
        one(
            "SELECT bytesize(gunzip(gzip(pad('', 524288)))) AS r FROM \"t/#\"",
            "{}"
        ),
        r#"{"r":524288}"#
    );
}

/// `bitsize`, `bytesize` and `subbits` as EMQX's source runs them. Values from EMQX 6.3.1.
#[test]
fn bit_sequence_functions_follow_emqx() {
    assert_eq!(val("bytesize(json_decode('[\"ab\",99]'))"), "3");
    refused("bytesize(1)");
    refused("bitsize(1)");
    for (expr, want) in [
        (
            "subbits(hexstr2bin('ABCD'), 1, 12, 'integer', 'unsigned', 'little')",
            "3243",
        ),
        (
            "subbits(hexstr2bin('ABCD'), 1, 12, 'integer', 'signed', 'little')",
            "-853",
        ),
        (
            "subbits(hexstr2bin('ABCDEF'), 1, 20, 'integer', 'unsigned', 'little')",
            "970155",
        ),
        (
            "subbits(hexstr2bin('0000803F'), 1, 32, 'float', 'unsigned', 'little')",
            "1.0",
        ),
        ("subbits(hexstr2bin('013C00'), 9, 32, 'float')", "1.0"),
        (
            "subbits(hexstr2bin('400921FB54442D18'), 1, 64, 'float')",
            "3.141592653589793",
        ),
        (
            "bin2hexstr(subbits(hexstr2bin('010203'), 9, 100, 'bits'))",
            r#""0203""#,
        ),
        (
            "bin2hexstr(subbits(hexstr2bin('010203'), 2, 8, 'bits'))",
            r#""02""#,
        ),
        (
            "subbits(hexstr2bin('FF'), 1, 100, 'integer', 'signed')",
            "-1",
        ),
        ("subbits(hexstr2bin('01'), 1, 0)", "0"),
        ("subbits(hexstr2bin('01'), 1, -1)", "1"),
        ("subbits(hexstr2bin('0102'), 9, 16)", "2"),
        ("subbits(hexstr2bin('000000000000000000FF'), 1, 80)", "255"),
        // Wider than 64 bits: an Erlang integer of any size.
        (
            "subbits(hexstr2bin('FFFFFFFFFFFFFFFFFF'), 1, 72)",
            "4722366482869645213695",
        ),
        (
            "subbits(hexstr2bin('FFFFFFFFFFFFFFFFFFFE'), 1, 80, 'integer', 'signed')",
            "-2",
        ),
        ("is_null(subbits(hexstr2bin('01'), 9, 4))", "true"),
        ("is_null(subbits(hexstr2bin('01'), 0, 8))", "true"),
        // A bad type is only looked at when the start is inside the binary.
        ("is_null(subbits(hexstr2bin('01'), 9, 8, 'foo'))", "true"),
    ] {
        assert_eq!(val(expr), want, "{expr}");
    }
    // The smallest half-precision subnormal, 2^-24 (compared in SQL: the literal parses
    // exactly).
    assert_eq!(
        val("subbits(hexstr2bin('0001'), 1, 16, 'float') = 5.960464477539063e-8"),
        "true"
    );
    for bad in [
        "subbits(hexstr2bin('7FC00000'), 1, 32, 'float')",
        "subbits(hexstr2bin('013C00'), 1, 8, 'float')",
        "subbits(hexstr2bin('01'), 1, 8, 'foo')",
        // Representable in Erlang, not here (docs/RULES.md).
        "subbits(hexstr2bin('FF'), 1, 4, 'bits')",
    ] {
        refused(bad);
    }
}

/// `getenv(Name)` reads `EMQXVAR_<Name>` and nothing else; an unset one is `''`.
#[test]
fn getenv_reads_only_the_emqxvar_namespace() {
    assert_eq!(val("getenv('MQTTD_TEST_NEVER_SET_ANYWHERE')"), r#""""#);
    // PATH is set in every test environment; only EMQXVAR_PATH would be read.
    assert_eq!(val("getenv('PATH')"), r#""""#);
    assert_eq!(val("getenv('')"), r#""""#);
    refused("getenv(1)");
    refused("getenv('A=B')");
}

/// `hash(Algorithm, Data)` offers every digest of `crypto:hash/2`. Values from EMQX
/// 6.3.1 for `'abc'`.
#[test]
fn hash_offers_every_digest_erlang_crypto_does() {
    for (alg, want) in [
        ("md4", "a448017aaf21d8525fc10ae87aa6729d"),
        ("md5", "900150983cd24fb0d6963f7d28e17f72"),
        ("sha", "a9993e364706816aba3e25717850c26c9cd0d89d"),
        ("sha1", "a9993e364706816aba3e25717850c26c9cd0d89d"),
        ("sha224", "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"),
        ("sha256", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        ("sha384", "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"),
        ("sha512", "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"),
        ("sha512_224", "4634270f707b6a54daae7530460842e20e37ed265ceee9a43e8924aa"),
        ("sha512_256", "53048e2681941ef99b2e29b76b4c7dabe4c2d0c634fc6d46e0e2f13107e7af23"),
        ("sha3_224", "e642824c3f8cf24ad09234ee7d3c766fc9a3a5168d0c94ad73b46fdf"),
        ("sha3_256", "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532"),
        ("sha3_384", "ec01498288516fc926459f58e2c6ad8df9b473cb0fc08c2596da7cf0e49be4b298d88cea927ac7f539f1edf228376d25"),
        ("sha3_512", "b751850b1a57168a5693cd924b6b096e08f621827444f70d884f5d0240d2712e10e116e9192af3c91a7ec57647e3934057340b4cf408d5a56592f8274eec53f0"),
        ("shake128", "5881092dd818bf5cf8a3ddb793fbcba7"),
        ("shake256", "483366601360a8771c6863080cc4114d8db44530f8f1e1ee4f94ea37e78b5739"),
        ("blake2b", "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923"),
        ("blake2s", "508c5e8c327c14e2e1a72ba34eeb452f37458b209ed63a294d999b4c86675982"),
        ("ripemd160", "8eb208f7e05d987a9b044a8e98c6b087f15a0bfc"),
        ("sm3", "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"),
    ] {
        assert_eq!(val(&format!("hash('{alg}', 'abc')")), format!("\"{want}\""), "{alg}");
    }
    assert_eq!(
        val("hash('md5', true)"),
        r#""b326b5062b2f0e69046810717534cb09""#
    );
    assert_eq!(
        val("hash('md5', json_decode('[\"a\",\"bc\"]'))"),
        r#""900150983cd24fb0d6963f7d28e17f72""#
    );
    refused("hash('nope', 'abc')");
    refused("hash('md5', 1)");
    refused("hash('md5', null())");
}

/// The sink helpers EMQX exposes as SQL functions. Values from EMQX 6.3.1.
#[test]
fn sink_helper_functions_follow_emqx() {
    assert_eq!(
        val(
            r#"map_to_redis_hset_args(json_decode('{"a":1,"b":1.5,"c":"x","d":true,"e":null,"f":[1],"g":{}}'))"#
        ),
        r#"["map_to_redis_hset_args","d","true","c","x","b","1.5","a","1"]"#
    );
    assert_eq!(
        val(r#"map_to_redis_hset_args('{"a":1,"b":2.0}')"#),
        r#"["map_to_redis_hset_args","b","2.0","a","1"]"#
    );
    for not_a_map in ["'nope'", "1", "'[1]'"] {
        assert_eq!(
            val(&format!("map_to_redis_hset_args({not_a_map})")),
            r#"["map_to_redis_hset_args"]"#
        );
    }
    assert_eq!(
        val(
            r#"join_to_sql_values_string(json_decode('["a''b",1,1.5,true,null,[1,2],{"a":1},"x\\y"]'))"#
        ),
        r#""'a\\'\\'b', 1, 1.5, 'true', 'null', '[1,2]', '{\"a\":1}', 'x\\\\y'""#
    );
    assert_eq!(
        val("join_to_sql_values_string(json_decode('[0.1,1.0e20,1.0,-0.5,12345678.123]'))"),
        r#""0.1, 100000000000000000000.0, 1.0, -0.5, 12345678.1229999997""#
    );
    assert_eq!(
        val("join_to_sql_values_string([payload.nope, 1])"),
        r#""NULL, 1""#
    );
    refused("join_to_sql_values_string(1)");
}

/// `div(a, b)` and `mod(a, b)`: EMQX's grammar calls the operators by name
/// (`div_or_mod '(' fun_args ')'`); `mod` is Erlang's `rem`. Values from EMQX 6.3.1.
#[test]
fn div_and_mod_can_be_called_as_functions() {
    assert_eq!(val("div(7, 2)"), "3");
    assert_eq!(val("div(-7, 2)"), "-3");
    assert_eq!(val("mod(-7, 2)"), "-1");
    assert_eq!(val("mod(7, -2)"), "1");
    assert_eq!(val("div(7, 2) + mod(7, 2) * 10"), "13");
    refused("div(7, 0)");
    refused("div(7.0, 2)");
    // Upper-case `DIV` is an ordinary (unknown) name in EMQX's lexer, as here.
    let e = check_sql("SELECT DIV(7, 2) FROM \"t\"").unwrap_err();
    assert!(e.contains("unknown function DIV()"), "{e}");
}

/// `is_empty`: `[]` and `''` are empty, any other array is not, and anything else goes
/// through EMQX's `map/1` — so a string must hold a JSON object. Values from EMQX 6.3.1.
#[test]
fn is_empty_reads_a_string_only_as_a_json_object() {
    assert_eq!(val("is_empty('')"), "true");
    assert_eq!(val("is_empty('{}')"), "true");
    assert_eq!(val(r#"is_empty('{"a":1}')"#), "false");
    for bad in ["'[]'", "'x'", "' '", "'null'", "1"] {
        refused(&format!("is_empty({bad})"));
    }
}

/// The smaller conversions and aliases EMQX's source has. Values from EMQX 6.3.1.
#[test]
fn hex_prefix_and_alias_functions_follow_emqx() {
    assert_eq!(val("bin2hexstr(hexstr2bin('abc'))"), r#""0ABC""#);
    assert_eq!(val("bin2hexstr('ab', '0x')"), r#""0x6162""#);
    assert_eq!(val("hexstr2bin('4142', payload.nope)"), r#""AB""#);
    refused("hexstr2bin('6162', '0x')");
    refused("bin2hexstr('ab', json_decode('null'))");
    assert_eq!(val("eq(json_decode('[1]'), json_decode('[1.0]'))"), "true");
    assert_eq!(val("eq(true, 'true')"), "false");
    assert_eq!(val("timezone_to_second('+08:00')"), "28800");
    assert_eq!(val("strlen(format_date('second', '+08:00', '%Y'))"), "4");
    refused("contains_topic('t/a', 't/a')");
}

// -- numbers: Erlang integers and floats, as EMQX 6.3.1 has them

/// Each `(expr, payload, want)` runs as `SELECT <expr> AS r FROM "t/#"`; `want` is the
/// exact JSON EMQX 6.3.1 renders for `r` (`emqx_rule_sqltester`, then jiffy), or `None`
/// where EMQX fails the rule.
fn emqx_says(cases: &[(&str, &str, Option<&str>)]) {
    let mut wrong = Vec::new();
    for (expr, payload, want) in cases {
        let sql = format!("SELECT {expr} AS r FROM \"t/#\"");
        let got = run_on(&sql, "t/a", payload).map(|out| {
            out.first()
                .and_then(|o| o.strip_prefix(r#"{"r":"#))
                .and_then(|o| o.strip_suffix('}'))
                .unwrap_or_default()
                .to_string()
        });
        match (want, &got) {
            (Some(w), Ok(g)) if g == w => {}
            (None, Err(_)) => {}
            _ => wrong.push(format!(
                "  {expr} (payload {payload}): EMQX {want:?}, mqttd {got:?}"
            )),
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} differ from EMQX 6.3.1:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// Integer literals of any size, and arithmetic past 64 bits: Erlang integers never overflow.
/// Every value is EMQX 6.3.1's (`emqx_rule_sqltester`), probed.
#[test]
#[allow(clippy::too_many_lines)]
fn integer_literals_and_arithmetic_never_overflow() {
    emqx_says(&[
        ("12345678901234567890", "{}", Some("12345678901234567890")),
        (
            "12345678901234567890 + 1",
            "{}",
            Some("12345678901234567891"),
        ),
        ("9223372036854775807 + 1", "{}", Some("9223372036854775808")),
        (
            "-9223372036854775808 - 1",
            "{}",
            Some("-9223372036854775809"),
        ),
        ("-9223372036854775808", "{}", Some("-9223372036854775808")),
        ("-(-9223372036854775808)", "{}", Some("9223372036854775808")),
        (
            "9223372036854775807 * 9223372036854775807",
            "{}",
            Some("85070591730234615847396907784232501249"),
        ),
        (
            "12345678901234567890 * 12345678901234567890 * 12345678901234567890",
            "{}",
            Some("1881676372353657772490265749424677022198701224860897069000"),
        ),
        (
            "12345678901234567890 - 12345678901234567889",
            "{}",
            Some("1"),
        ),
        (
            "12345678901234567890 div 7",
            "{}",
            Some("1763668414462081127"),
        ),
        (
            "-12345678901234567890 div 7",
            "{}",
            Some("-1763668414462081127"),
        ),
        ("12345678901234567890 mod 7", "{}", Some("1")),
        ("-12345678901234567890 mod 7", "{}", Some("-1")),
        ("12345678901234567890 mod -7", "{}", Some("1")),
        (
            "-9223372036854775808 div -1",
            "{}",
            Some("9223372036854775808"),
        ),
        ("-9223372036854775808 mod -1", "{}", Some("0")),
        (
            "12345678901234567890 / 1",
            "{}",
            Some("1.2345678901234567e19"),
        ),
        (
            "12345678901234567890 / 12345678901234567890",
            "{}",
            Some("1.0"),
        ),
        ("7 / 2", "{}", Some("3.5")),
        ("1 / 3", "{}", Some("0.3333333333333333")),
        (
            "12345678901234567890 + 0.5",
            "{}",
            Some("1.2345678901234567e19"),
        ),
        ("9007199254740993 + 0.0", "{}", Some("9.007199254740992e15")),
        ("9007199254740993 * 1.0", "{}", Some("9.007199254740992e15")),
        ("12345678901234567890 div 0", "{}", None),
        ("12345678901234567890 / 0", "{}", None),
        ("1 / 0.0", "{}", None),
        ("1.0e308 * 10", "{}", None),
        ("-payload.n", r#"{"n":-0.0}"#, Some("0.0")),
        ("-payload.n", r#"{"n":0.0}"#, Some("0.0")),
        (
            "-payload.n",
            r#"{"n":9223372036854775808}"#,
            Some("-9223372036854775808"),
        ),
        (
            "-payload.n",
            r#"{"n":-9223372036854775808}"#,
            Some("9223372036854775808"),
        ),
    ]);
}

/// JSON numbers in and out, as jiffy decodes and encodes them: integers exact at any size, floats the nearest double, written in Erlang's shortest form.
/// Every value is EMQX 6.3.1's (`emqx_rule_sqltester`), probed.
#[test]
#[allow(clippy::too_many_lines)]
fn json_numbers_decode_and_encode_as_jiffy_does() {
    emqx_says(&[
        (
            "payload.n",
            r#"{"n":12345678901234567890}"#,
            Some("12345678901234567890"),
        ),
        (
            "payload.n + 1",
            r#"{"n":12345678901234567890}"#,
            Some("12345678901234567891"),
        ),
        (
            "payload.n",
            r#"{"n":-12345678901234567890123}"#,
            Some("-12345678901234567890123"),
        ),
        (
            "payload.n * 2",
            r#"{"n":-12345678901234567890123}"#,
            Some("-24691357802469135780246"),
        ),
        (
            "payload.n",
            r#"{"n":18446744073709551616}"#,
            Some("18446744073709551616"),
        ),
        (
            "payload.n",
            r#"{"n":9223372036854775807}"#,
            Some("9223372036854775807"),
        ),
        (
            "payload.n",
            r#"{"n":9223372036854775808}"#,
            Some("9223372036854775808"),
        ),
        (
            "payload.n",
            r#"{"n":-9223372036854775808}"#,
            Some("-9223372036854775808"),
        ),
        (
            "payload.n",
            r#"{"n":-9223372036854775809}"#,
            Some("-9223372036854775809"),
        ),
        ("payload.n", r#"{"n":-0}"#, Some("0")),
        ("payload.n", r#"{"n":-0.0}"#, Some("0.0")),
        (
            "payload.n",
            r#"{"n":5.960464477539063e-8}"#,
            Some("5.960464477539063e-8"),
        ),
        ("payload.n", r#"{"n":0.1}"#, Some("0.1")),
        ("payload.n", r#"{"n":1e5}"#, Some("1.0e5")),
        ("payload.n", r#"{"n":1E-5}"#, Some("1.0e-5")),
        (
            "payload.n",
            r#"{"n":123456789012345678901234567890e-10}"#,
            Some("1.2345678901234567e19"),
        ),
        ("payload.n", r#"{"n":1.0e400}"#, None),
        ("payload.n", r#"{"n":1.0e-400}"#, Some("0.0")),
        ("payload.n", r#"{"n":-1.0e-400}"#, Some("0.0")),
        (
            "payload.n",
            r#"{"n":2.2250738585072011e-308}"#,
            Some("2.225073858507201e-308"),
        ),
        (
            "payload.n",
            r#"{"n":4.9406564584124654e-324}"#,
            Some("5.0e-324"),
        ),
        (
            "payload.n",
            r#"{"n":1.7976931348623157e308}"#,
            Some("1.7976931348623157e308"),
        ),
        (
            "payload.n",
            r#"{"n":9007199254740993}"#,
            Some("9007199254740993"),
        ),
        (
            "payload.n",
            r#"{"n":9007199254740993.0}"#,
            Some("9.007199254740992e15"),
        ),
        (
            "payload.n",
            r#"{"n":0.30000000000000004}"#,
            Some("0.30000000000000004"),
        ),
        ("payload.n", r#"{"n":100}"#, Some("100")),
        ("payload.n", r#"{"n":100.0}"#, Some("100.0")),
        ("payload.n", r#"{"n":1000.0}"#, Some("1.0e3")),
        ("payload.n", r#"{"n":1.5e300}"#, Some("1.5e300")),
        ("payload.n", r#"{"n":12.5e-1}"#, Some("1.25")),
        ("payload.n", r#"{"n":[1,2.5,-3e2]}"#, Some("[1,2.5,-300.0]")),
        (
            "str(payload.n)",
            r#"{"n":12345678901234567890}"#,
            Some(r#""12345678901234567890""#),
        ),
        (
            "json_encode(payload)",
            r#"{"n":12345678901234567890,"f":1e20,"z":-0.0}"#,
            Some(r#""\"{\\\"n\\\":12345678901234567890,\\\"f\\\":1e20,\\\"z\\\":-0.0}\"""#),
        ),
        (
            "payload.n",
            r#"{"n":"x","m":1.2345678901234567e19}"#,
            Some(r#""x""#),
        ),
        (
            "payload.m",
            r#"{"m":1.2345678901234567e19}"#,
            Some("1.2345678901234567e19"),
        ),
        (
            "payload.m",
            r#"{"m":12345678901234567168}"#,
            Some("12345678901234567168"),
        ),
    ]);
}

/// Comparisons: an integer against a float by exact value, against a string by Erlang's own number syntax; `=:=` (`CASE`, `contains`) keeps integers, floats and `-0.0` apart.
/// Every value is EMQX 6.3.1's (`emqx_rule_sqltester`), probed.
#[test]
#[allow(clippy::too_many_lines)]
fn numbers_compare_by_value_as_erlang_does() {
    emqx_says(&[
        (
            "payload.n > 12345678901234567889.0",
            r#"{"n":12345678901234567890}"#,
            Some("true"),
        ),
        (
            "payload.n = 12345678901234567890",
            r#"{"n":12345678901234567890}"#,
            Some("true"),
        ),
        (
            "is_int(payload.n)",
            r#"{"n":12345678901234567890}"#,
            Some("true"),
        ),
        (
            "is_float(payload.n)",
            r#"{"n":12345678901234567890}"#,
            Some("false"),
        ),
        (
            "is_num(payload.n)",
            r#"{"n":12345678901234567890}"#,
            Some("true"),
        ),
        (
            "12345678901234567890 = 12345678901234567890.0",
            "{}",
            Some("false"),
        ),
        ("12345678901234567890 > 1.0e19", "{}", Some("true")),
        ("12345678901234567890 < 1.3e19", "{}", Some("true")),
        ("9007199254740993 = 9007199254740992.0", "{}", Some("false")),
        ("9007199254740993 > 9007199254740992.0", "{}", Some("true")),
        ("9007199254740992 = 9007199254740992.0", "{}", Some("true")),
        (
            "-9007199254740993 < -9007199254740992.0",
            "{}",
            Some("true"),
        ),
        ("1 = 1.0", "{}", Some("true")),
        ("0.0 = -0.0", "{}", Some("true")),
        ("0.5 > 0", "{}", Some("true")),
        ("-0.5 < 0", "{}", Some("true")),
        (
            "12345678901234567890 = '12345678901234567890'",
            "{}",
            Some("true"),
        ),
        (
            "12345678901234567890 > '12345678901234567889'",
            "{}",
            Some("true"),
        ),
        ("12345678901234567890 > '1.0e19'", "{}", Some("true")),
        ("5 = '5.0'", "{}", Some("true")),
        ("5 = '+5'", "{}", Some("true")),
        ("5 = ' 5'", "{}", None),
        ("5 = '1e1'", "{}", None),
        ("5 = '5.'", "{}", None),
        ("12345678901234567890 > 'abc'", "{}", None),
        ("12345678901234567890 > true", "{}", Some("false")),
        ("12345678901234567890 < 'abc'", "{}", None),
        (
            "[1,12345678901234567890,3]",
            "{}",
            Some("[1,12345678901234567890,3]"),
        ),
        (
            "contains(12345678901234567890, [12345678901234567890])",
            "{}",
            Some("true"),
        ),
        ("contains(1, [1.0])", "{}", Some("false")),
        ("contains(-0.0, [0.0])", "{}", Some("false")),
        (
            "CASE 12345678901234567890 WHEN 12345678901234567890 THEN 'y' ELSE 'n' END",
            "{}",
            Some(r#""y""#),
        ),
        (
            "CASE 1 WHEN 1.0 THEN 'y' ELSE 'n' END",
            "{}",
            Some(r#""n""#),
        ),
        (
            "payload.a[12345678901234567890]",
            r#"{"a":[1,2]}"#,
            Some(r#""undefined""#),
        ),
        (
            "eq(12345678901234567890, 12345678901234567890.0)",
            "{}",
            Some("false"),
        ),
        ("eq(1, 1.0)", "{}", Some("true")),
    ]);
}

/// Floats as text: `str`, templates and concatenation print with ten decimals as the Erlang VM does (`-0.0` keeps its sign, past 255 characters it fails); JSON prints the shortest form (`-0.0` as `0.0`).
/// Every value is EMQX 6.3.1's (`emqx_rule_sqltester`), probed.
#[test]
#[allow(clippy::too_many_lines)]
fn float_text_is_erlangs() {
    emqx_says(&[
        ("str(-0.0)", "{}", Some(r#""-0.0""#)),
        ("str(1.0e-11)", "{}", Some(r#""0.0""#)),
        ("str(-1.0e-11)", "{}", Some(r#""-0.0""#)),
        ("str(5.0e-11)", "{}", Some(r#""0.0000000001""#)),
        ("str(0.1 + 0.2)", "{}", Some(r#""0.3""#)),
        ("str(1.0e20)", "{}", Some(r#""100000000000000000000.0""#)),
        ("str(1.0e250)", "{}", None),
        (
            "str(12345678901234567890)",
            "{}",
            Some(r#""12345678901234567890""#),
        ),
        (
            "str(-12345678901234567890)",
            "{}",
            Some(r#""-12345678901234567890""#),
        ),
        ("str(0.00048828125)", "{}", Some(r#""0.0004882813""#)),
        (
            "'x' + 12345678901234567890",
            "{}",
            Some(r#""x12345678901234567890""#),
        ),
        ("'x' + 1.0e300", "{}", None),
        ("'x' + 1.0e-7", "{}", Some(r#""x0.0000001""#)),
        ("concat(1.0e-11, -0.0)", "{}", Some(r#""0.0-0.0""#)),
        (
            "json_encode(12345678901234567890)",
            "{}",
            Some(r#""12345678901234567890""#),
        ),
        ("json_encode(-0.0)", "{}", Some(r#""0.0""#)),
        ("json_encode(1.0e20)", "{}", Some(r#""1.0e20""#)),
        (
            "json_encode([1.0e15, 1.0e16, 100.0, 1000.0, 0.0001, 0.00001, 123456789012.5])",
            "{}",
            Some(r#""[1.0e15,1.0e16,100.0,1.0e3,0.0001,1.0e-5,123456789012.5]""#),
        ),
        (
            "json_decode('12345678901234567890123')",
            "{}",
            Some("12345678901234567890123"),
        ),
        ("json_decode('[1.0e400]')", "{}", None),
        ("-0.0", "{}", Some("0.0")),
        ("0.1 + 0.2", "{}", Some("0.30000000000000004")),
        ("1.0e22", "{}", Some("1.0e22")),
        ("1.0e23", "{}", Some("1.0e23")),
        ("5.0e-324", "{}", Some("5.0e-324")),
        ("9007199254740992.0", "{}", Some("9.007199254740992e15")),
        ("9007199254740991.0", "{}", Some("9007199254740991.0")),
        ("4503599627370496.5", "{}", Some("4503599627370496.0")),
        ("123456789.0", "{}", Some("123456789.0")),
        ("1234567890.0", "{}", Some("1234567890.0")),
        ("12345678901.0", "{}", Some("12345678901.0")),
        ("1.0e-7", "{}", Some("1.0e-7")),
        (
            "str(967562026147900.25)",
            "{}",
            Some(r#""967562026147900.25""#),
        ),
        ("967562026147900.25", "{}", Some("967562026147900.2")),
        (
            "payload.n",
            r#"{"n":967562026147900.25}"#,
            Some("967562026147900.2"),
        ),
        (
            "payload.n",
            r#"{"n":4503599627370497.5}"#,
            Some("4503599627370498.0"),
        ),
        ("str(1.0e15 + 0.3)", "{}", Some(r#""1000000000000000.25""#)),
        ("float2str(1.0e-7, 20)", "{}", Some(r#""0.0000001""#)),
        (
            "float2str(0.1, 19)",
            "{}",
            Some(r#""0.1000000000000000056""#),
        ),
        (
            "float2str(9007199254740993.0, 2)",
            "{}",
            Some(r#""9007199254740992.0""#),
        ),
        (
            "float2str(0.000000000095367431640625, 21)",
            "{}",
            Some(r#""0.000000000095367431641""#),
        ),
    ]);
}

/// Every numeric function at values past 64 bits, and at the float edges EMQX treats specially.
/// Every value is EMQX 6.3.1's (`emqx_rule_sqltester`), probed.
#[test]
#[allow(clippy::too_many_lines)]
fn numeric_functions_take_integers_of_any_size() {
    emqx_says(&[
        ("abs(-12345678901234567890)", "{}", Some("12345678901234567890")),
        ("abs(-9223372036854775808)", "{}", Some("9223372036854775808")),
        ("abs(-1.5)", "{}", None),
        ("abs(-3)", "{}", Some("3")),
        ("ceil(1.0e20)", "{}", Some("100000000000000000000")),
        ("floor(-1.0e20)", "{}", Some("-100000000000000000000")),
        ("round(1.0e20)", "{}", Some("100000000000000000000")),
        ("round(2.5)", "{}", Some("3")),
        ("round(-2.5)", "{}", Some("-3")),
        ("round(0.49999999999999994)", "{}", Some("0")),
        ("round(9007199254740993)", "{}", Some("9007199254740993")),
        ("ceil(9007199254740993)", "{}", Some("9007199254740993")),
        ("floor(-12345678901234567890)", "{}", Some("-12345678901234567890")),
        ("ceil(-0.5)", "{}", Some("0")),
        ("round(-0.4)", "{}", Some("0")),
        ("ceil(1.7976931348623157e308)", "{}", Some("179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368")),
        ("int(12345678901234567890)", "{}", Some("12345678901234567890")),
        ("int(1.0e20)", "{}", Some("100000000000000000000")),
        ("int(-1.5)", "{}", Some("-2")),
        ("int('12345678901234567890')", "{}", Some("12345678901234567890")),
        ("int('-12345678901234567890123')", "{}", Some("-12345678901234567890123")),
        ("int('1.0e20')", "{}", Some("100000000000000000000")),
        ("int('1.5E3')", "{}", Some("1500")),
        ("int('+12')", "{}", Some("12")),
        ("int('0012')", "{}", Some("12")),
        ("int(' 12')", "{}", None),
        ("int('1e5')", "{}", None),
        ("int('1_000')", "{}", None),
        ("int('1.e5')", "{}", None),
        ("int('1.0e400')", "{}", None),
        ("int('-1.0e-400')", "{}", Some("0")),
        ("int(true)", "{}", Some("1")),
        ("float(12345678901234567890)", "{}", Some("1.2345678901234567e19")),
        ("float(123456789012345678901234567890123456789)", "{}", Some("1.2345678901234568e38")),
        ("float(36893488147419107329)", "{}", Some("3.689348814741911e19")),
        ("float(340282366920938463463374607431768211457)", "{}", Some("3.402823669209385e38")),
        ("float('12345678901234567890123456789')", "{}", Some("1.2345678901234568e28")),
        ("float('12')", "{}", Some("12.0")),
        ("float('+1.5')", "{}", Some("1.5")),
        ("float('007.50')", "{}", Some("7.5")),
        ("float('1.5e+3')", "{}", Some("1.5e3")),
        ("float('1.0e-400')", "{}", Some("0.0")),
        ("float('-1.0e-400')", "{}", Some("0.0")),
        ("float('1e5')", "{}", None),
        ("float(' 1.5')", "{}", None),
        ("float('.5')", "{}", None),
        ("float('1.0e400')", "{}", None),
        ("float(12345678901234567890, 3)", "{}", Some("1.2345678901234567e19")),
        ("float(0.125, 2)", "{}", Some("0.13")),
        ("float(-0.0, 1)", "{}", Some("0.0")),
        ("float(1.5, 253)", "{}", Some("1.5")),
        ("float(1.5, 254)", "{}", None),
        ("float(1.0e20, 2)", "{}", Some("1.0e20")),
        ("float(2.5, 0)", "{}", None),
        ("float2str(12345678901234567890.0, 2)", "{}", Some(r#""12345678901234567168.0""#)),
        ("float2str(0.125, 2)", "{}", Some(r#""0.13""#)),
        ("float2str(2.5, 0)", "{}", Some(r#""3""#)),
        ("float2str(-0.0, 3)", "{}", Some(r#""-0.0""#)),
        ("float2str(1.0e20, 2)", "{}", Some(r#""100000000000000000000.0""#)),
        ("float2str(5, 2)", "{}", None),
        ("float2str(0.000000001, 5)", "{}", Some(r#""0.0""#)),
        ("float2str(-0.000000001, 5)", "{}", Some(r#""-0.0""#)),
        ("float2str(1.0e243, 10)", "{}", Some(r#""1000000000000000074650575649831695774632795300119615593163034400120115457135799236292149453307499328074479031320129942191467592834574340826335964513506590066150788638749118835418037019527222886944981240519484646566146722558989084608335389392896.0""#)),
        ("float2str(2.0e243, 11)", "{}", None),
        ("power(12345678901234567890, 2)", "{}", Some("1.5241578753238834e38")),
        ("power(2, 0.5)", "{}", Some("1.4142135623730951")),
        ("power(2, 10)", "{}", Some("1024.0")),
        ("power(10, 400)", "{}", None),
        ("sqrt(12345678901234567890)", "{}", Some("3513641828.820144")),
        ("fmod(12345678901234567890, 7)", "{}", Some("0.0")),
        ("exp(12345678901234567890)", "{}", None),
        ("log(123456789012345678901234567890123456789012345678901234567890)", "{}", Some("136.06324150896435")),
        ("sin(12345678901234567890)", "{}", Some("0.8952062890824876")),
        ("bitnot(12345678901234567890)", "{}", Some("-12345678901234567891")),
        ("bitnot(9223372036854775807)", "{}", Some("-9223372036854775808")),
        ("bitnot(-9223372036854775809)", "{}", Some("9223372036854775808")),
        ("bitand(12345678901234567890, 255)", "{}", Some("210")),
        ("bitand(-1, 12345678901234567890)", "{}", Some("12345678901234567890")),
        ("bitand(-12345678901234567890, 12345678901234567890)", "{}", Some("2")),
        ("bitor(12345678901234567890, 1)", "{}", Some("12345678901234567891")),
        ("bitor(-12345678901234567890, 1)", "{}", Some("-12345678901234567889")),
        ("bitxor(12345678901234567890, 12345678901234567890)", "{}", Some("0")),
        ("bitxor(-1, 18446744073709551615)", "{}", Some("-18446744073709551616")),
        ("bitsl(1, 100)", "{}", Some("1267650600228229401496703205376")),
        ("bitsl(1, 64)", "{}", Some("18446744073709551616")),
        ("bitsl(1, 63)", "{}", Some("9223372036854775808")),
        ("bitsl(1, -1)", "{}", Some("0")),
        ("bitsl(-1, 3)", "{}", Some("-8")),
        ("bitsl(3, 0)", "{}", Some("3")),
        ("bitsl(-5, 70)", "{}", Some("-5902958103587056517120")),
        ("bitsr(12345678901234567890, 3)", "{}", Some("1543209862654320986")),
        ("bitsr(-12345678901234567890, 100)", "{}", Some("-1")),
        ("bitsr(1, -3)", "{}", Some("8")),
        ("bitsr(5, 100)", "{}", Some("0")),
        ("bitsr(-5, 1)", "{}", Some("-3")),
        ("bitsr(-1, 12345678901234567890)", "{}", Some("-1")),
        ("bitsr(1, 12345678901234567890)", "{}", Some("0")),
        ("bitsl(0, 12345678901234567890)", "{}", Some("0")),
        ("div(12345678901234567890, 7)", "{}", Some("1763668414462081127")),
        ("mod(-12345678901234567890, 7)", "{}", Some("-1")),
        ("div(7, 2.0)", "{}", None),
        ("map_to_range(12345678901234567890, 1, 10)", "{}", Some("1")),
        ("map_to_range(-12345678901234567890, 1, 10)", "{}", Some("1")),
        ("map_to_range(-3, 1, 10)", "{}", Some("-2")),
        ("map_to_range(-3, -5, 5)", "{}", Some("-8")),
        ("map_to_range(13, 1, 10)", "{}", Some("4")),
        ("map_to_range('abc', -12345678901234567890, 12345678901234567890)", "{}", Some("-12345678901228185711")),
        ("map_to_range('abc', 1, 10)", "{}", Some("10")),
        ("hash_to_range('a', 1, 12345678901234567890)", "{}", Some("3072281523061963130")),
        ("hash_to_range('a', 1, 10)", "{}", Some("10")),
        ("hash_to_range('', 1, 10)", "{}", None),
        ("subbits('abcdefghijklmnopqrstuvwxyz', 1, 128)", "{}", Some("129445976596022050476432668810952994672")),
        ("subbits('abcdefghijklmnopqrstuvwxyz', 1, 72, 'integer', 'signed', 'little')", "{}", Some("1944431222027710587489")),
        ("subbits('abcdefghijklmnopqrstuvwxyz', 1, 72, 'integer', 'unsigned', 'little')", "{}", Some("1944431222027710587489")),
        ("subbits('abcdefghijklmnopqrstuvwxyz', 9, 100, 'integer', 'signed', 'big')", "{}", Some("487195019612513355983146632918")),
        ("bool(12345678901234567890)", "{}", None),
        ("bool(1.0)", "{}", Some("true")),
        ("bool(0.0)", "{}", Some("false")),
        ("bool(-0.0)", "{}", Some("false")),
        ("bool(1)", "{}", Some("true")),
        ("bytesize([12345678901234567890])", "{}", None),
        ("nth(12345678901234567890, [1])", "{}", None),
        ("sublist(12345678901234567890, [1,2])", "{}", Some("[1,2]")),
        ("sublist(1, 12345678901234567890, [1,2])", "{}", Some("[1,2]")),
        ("substr('abc', 12345678901234567890)", "{}", Some(r#""""#)),
        ("substr('abc', 1, 12345678901234567890)", "{}", Some(r#""bc""#)),
        ("sprintf('~p ~w ~b', 12345678901234567890, -12345678901234567890, 12345678901234567890)", "{}", Some(r#""12345678901234567890 -12345678901234567890 12345678901234567890""#)),
        ("sprintf('~c', -12345678901234567890)", "{}", Some(r#"".""#)),
        ("sprintf('~p|~w', 1.0e20, -0.0)", "{}", Some(r#""1.0e20|-0.0""#)),
        ("sprintf('~30W', 12345678901234567890, 2)", "{}", Some(r#""          12345678901234567890""#)),
        ("sprintf('~5p', 12345678901234567890)", "{}", Some(r#""12345678901234567890""#)),
        ("sprintf('~s', 12345678901234567890)", "{}", None),
        ("sprintf('~f|~e|~g', -0.0, -0.0, -0.0)", "{}", Some(r#""-0.000000|-0.00000e+0|-0.00000e+0""#)),
        ("sprintf('~f', 12345678901234567890)", "{}", None),
        ("sprintf('~10.3f', 12345678901234567890.0)", "{}", Some(r#""**********""#)),
        ("join_to_sql_values_string([12345678901234567890, 1.0e20, -0.0, 0.1])", "{}", Some(r#""12345678901234567890, 100000000000000000000.0, -0.0, 0.1""#)),
        (r#"map_to_redis_hset_args(json_decode('{"a":12345678901234567890,"b":1.0e20,"c":-0.0,"d":0.1234567}'))"#, "{}", Some(r#"["map_to_redis_hset_args","d","0.123457","c","-0.0","b","100000000000000000000.0","a","12345678901234567890"]"#)),
        ("float(70889591166011248673)", "{}", Some("7.0889591166011245e19")),
        ("70889591166011248673 + 0.0", "{}", Some("7.0889591166011245e19")),
        ("float2str(0.995, 2)", "{}", Some(r#""1.0""#)),
        ("float2str(1.0e20, 0)", "{}", Some(r#""1""#)),
        ("float2str(12345.0, 0)", "{}", Some(r#""12345""#)),
        ("float2str(0.5, 0)", "{}", Some(r#""1""#)),
        ("float(0.995, 2)", "{}", Some("1.0")),
        ("float(bitsl(1, 1024))", "{}", None),
        ("bitsl(1, 1024) / 1", "{}", None),
        ("bitsl(1, 1023) * 1.0", "{}", Some("8.98846567431158e307")),
        ("float(bitsl(1, 1024) - 1)", "{}", None),
        ("sqrt(bitsl(1, 1024))", "{}", None),
    ]);
}

/// `IN` is Erlang's `lists:member`: exact (`=:=`), so `1` is not in `(1.0)`, `-0.0` not
/// in `(0.0)`, and `[1.0]` not in `([1])`. EMQX 6.3.1, probed in a `WHERE`.
#[test]
fn in_is_exact_membership() {
    for (cond, payload, passes) in [
        ("12345678901234567890 IN (12345678901234567890)", "{}", true),
        ("1 IN (1.0)", "{}", false),
        ("-0.0 IN (0.0)", "{}", false),
        ("0.0 IN (0.0)", "{}", true),
        ("payload.a IN (1)", r#"{"a":1.0}"#, false),
        ("payload.a IN ([1])", r#"{"a":[1.0]}"#, false),
        ("payload.a IN ([1.0])", r#"{"a":[1.0]}"#, true),
        (
            "payload.n IN (12345678901234567890, 2)",
            r#"{"n":12345678901234567890}"#,
            true,
        ),
        (
            "payload.n > 9007199254740992.0",
            r#"{"n":9007199254740993}"#,
            true,
        ),
        (
            "payload.n = 9007199254740992.0",
            r#"{"n":9007199254740993}"#,
            false,
        ),
    ] {
        let sql = format!("SELECT 1 AS r FROM \"t/#\" WHERE {cond}");
        let out = run_on(&sql, "t/a", payload).unwrap();
        assert_eq!(out.len(), usize::from(passes), "{cond} on {payload}");
    }
}

/// Floats printed bit for bit as EMQX 6.3.1 prints them, over the cases where a
/// textbook algorithm would not: Ryu's tie to even in the shortest form, the Erlang VM's
/// floating-point rounding of `{decimals, D}` (`1.5e-10` to ten decimals is
/// `0.0000000002`), its `compact` trimming an integer's zeros (`float2str(1.0e16, 0)` is
/// `"1"`), and its 255-character limit. Columns: the bits; JSON; `str()` (ten decimals,
/// compact); three decimals, compact; no decimals, compact (`ERR`: EMQX fails).
#[test]
#[allow(clippy::too_many_lines)]
fn floats_print_as_erlang_does_bit_for_bit() {
    const ROWS: &[(&str, &str, &str, &str, &str)] = &[
        ("3de49da7e361ce4c", "1.5e-10", "0.0000000002", "0.0", "0"),
        (
            "4341c37937e08000",
            "1.0e16",
            "10000000000000000.0",
            "10000000000000000.0",
            "1",
        ),
        (
            "4484ea15b273b38a",
            "1.2345678901234568e22",
            "12345678901234567741440.0",
            "12345678901234567741440.0",
            "1234567890123456774144",
        ),
        (
            "45208abdc094a705",
            "9.999e24",
            "9999000000000000346030080.0",
            "9999000000000000346030080.0",
            "999900000000000034603008",
        ),
        (
            "430b7ff0b6ef01e2",
            "967562026147900.2",
            "967562026147900.25",
            "967562026147900.25",
            "967562026147900",
        ),
        (
            "c31ee81b057f9111",
            "-2174863013831748.2",
            "-2174863013831748.25",
            "-2174863013831748.25",
            "-2174863013831748",
        ),
        (
            "7fefffffffffffff",
            "1.7976931348623157e308",
            "ERR",
            "ERR",
            "ERR",
        ),
        (
            "f8403c2e427ee78b",
            "-1.7153808869955908e271",
            "ERR",
            "ERR",
            "ERR",
        ),
        (
            "c2443f630e1658dd",
            "-173925604396.69424",
            "-173925604396.6942443848",
            "-173925604396.694",
            "-173925604397",
        ),
        (
            "bc80fd91b0561d70",
            "-2.947382665166891e-17",
            "-0.0",
            "-0.0",
            "-0",
        ),
        (
            "3ed04d7118ae9036",
            "3.8868205462263175e-6",
            "0.0000038868",
            "0.0",
            "0",
        ),
        (
            "415f3f660c7a3f95",
            "8191384.194961448",
            "8191384.1949614482",
            "8191384.195",
            "8191384",
        ),
        (
            "c1cad0914a4918bc",
            "-899752596.5710673",
            "-899752596.5710673332",
            "-899752596.571",
            "-899752597",
        ),
        (
            "c34abedf9418c299",
            "-1.5056433732224306e16",
            "-15056433732224306.0",
            "-15056433732224306.0",
            "-15056433732224306",
        ),
        (
            "bdf82b116bc50e16",
            "-3.5169410072545727e-10",
            "-0.0000000004",
            "-0.0",
            "-0",
        ),
        (
            "4302263c757d1646",
            "638573836477128.8",
            "638573836477128.75",
            "638573836477128.75",
            "638573836477129",
        ),
        (
            "4300490f1f9224fb",
            "572991116297375.4",
            "572991116297375.375",
            "572991116297375.375",
            "572991116297375",
        ),
        (
            "433e8ea3c016a868",
            "8601083254843496.0",
            "8601083254843496.0",
            "8601083254843496.0",
            "8601083254843496",
        ),
        (
            "43928fceff7ea46d",
            "3.343788027722535e17",
            "334378802772253504.0",
            "334378802772253504.0",
            "334378802772253504",
        ),
        (
            "43f45ad632874412",
            "2.346752225869779e19",
            "23467522258697789440.0",
            "23467522258697789440.0",
            "2346752225869778944",
        ),
        (
            "44b2a6dc789f37d1",
            "8.808064283063703e22",
            "88080642830637027295232.0",
            "88080642830637027295232.0",
            "88080642830637027295232",
        ),
        (
            "41c8cdcc63992000",
            "832280775.1962891",
            "832280775.1962890625",
            "832280775.196",
            "832280775",
        ),
        (
            "c30faf1f8b092ea3",
            "-1114784286189012.4",
            "-1114784286189012.375",
            "-1114784286189012.375",
            "-1114784286189012",
        ),
        ("3da5fd7fe1796495", "1.0e-11", "0.0", "0.0", "0"),
        ("3dcb7cdfd9d7bdbb", "5.0e-11", "0.0000000001", "0.0", "0"),
        ("3ee4f8b588e368f1", "1.0e-5", "0.00001", "0.0", "0"),
        ("3f1a36e2eb1c432d", "0.0001", "0.0001", "0.0", "0"),
        ("4059000000000000", "100.0", "100.0", "100.0", "100"),
        ("408f400000000000", "1.0e3", "1000.0", "1000.0", "1000"),
        (
            "430c6bf526340000",
            "1.0e15",
            "1000000000000000.0",
            "1000000000000000.0",
            "1000000000000000",
        ),
        (
            "4480f0cf064dd592",
            "1.0e22",
            "10000000000000000000000.0",
            "10000000000000000000000.0",
            "1",
        ),
        (
            "4340000000000000",
            "9.007199254740992e15",
            "9007199254740992.0",
            "9007199254740992.0",
            "9007199254740992",
        ),
        ("3fc0000000000000", "0.125", "0.125", "0.125", "0"),
        ("4004000000000000", "2.5", "2.5", "2.5", "3"),
        ("3fc3333333333333", "0.15", "0.15", "0.15", "0"),
        ("3ff0020c49ba5e35", "1.0005", "1.0005", "1.0", "1"),
        ("4000010624dd2f1b", "2.0005", "2.0005", "2.001", "2"),
        (
            "44b52d02c7e14af6",
            "1.0e23",
            "99999999999999991611392.0",
            "99999999999999991611392.0",
            "99999999999999991611392",
        ),
        ("0000000000000001", "5.0e-324", "0.0", "0.0", "0"),
        (
            "0010000000000000",
            "2.2250738585072014e-308",
            "0.0",
            "0.0",
            "0",
        ),
        ("bfc0000000000000", "-0.125", "-0.125", "-0.125", "-0"),
    ];
    for &(bits, json, ten, three, zero) in ROWS {
        let f = f64::from_bits(u64::from_str_radix(bits, 16).unwrap());
        assert_eq!(Value::Float(f).to_json().unwrap(), json, "{bits}");
        for (d, want) in [(10, ten), (3, three), (0, zero)] {
            let got = crate::num::float_to_decimals(f, d, true).unwrap_or_else(|_| "ERR".into());
            assert_eq!(got, want, "{bits} with {d} decimals");
        }
    }
}

/// JSON floats read as jiffy reads them (the nearest double; `ERR`: out of range), and
/// integers past 64 bits converted to floats as the Erlang VM converts them — digit by
/// 64-bit digit, so not always to the nearest double (`70889591166011248673` is
/// `7.0889591166011245e19`, not `…25e19`). Bits from EMQX 6.3.1.
#[test]
fn floats_parse_and_convert_as_erlang_does() {
    for (text, bits) in [
        ("256136027328.3830870998165e44", "4b70b6b222beb45b"),
        ("278141740925958309382024.383125e147", "63526ccfe21ebe66"),
        (
            "66937818471833154147735355.3154716640029685e-21",
            "40f0579d1875ebc2",
        ),
        ("86471.127e-169", "1ddfde7837eb25c8"),
        ("356.1e189", "67b3fb1b42bc504f"),
        ("44824526448.2955933e-226", "1338b941219c976f"),
        ("5.960464477539063e-8", "3e70000000000000"),
        ("2.2250738585072011e-308", "000fffffffffffff"),
        ("2.2250738585072012e-308", "0010000000000000"),
        ("4.9406564584124654e-324", "0000000000000001"),
        ("2.4703282292062328e-324", "0000000000000001"),
        ("1.7976931348623158e308", "7fefffffffffffff"),
        ("9007199254740993.0", "4340000000000000"),
        ("0.30000000000000004", "3fd3333333333334"),
        (
            "1.00000000000000011102230246251565404236316680908203125",
            "3ff0000000000000",
        ),
        (
            "1.00000000000000011102230246251565404236316680908203124",
            "3ff0000000000000",
        ),
        (
            "1.00000000000000011102230246251565404236316680908203126",
            "3ff0000000000001",
        ),
        ("1.0e400", "ERR"),
        ("-1.0e400", "ERR"),
    ] {
        let got = match json_decode(text.as_bytes()) {
            Ok(Value::Float(f)) => format!("{:016x}", f.to_bits()),
            _ => "ERR".to_string(),
        };
        assert_eq!(got, bits, "{text}");
    }
    for (n, bits) in [
        ("20285150958590414285410241185645068675621062784383904495341184577860372554048981890416682625969822291901411300849365357151810608330771060286153", "5d7a9da8138a374d"),
        ("67296193398529983120953729518316948107003529006333423211957430508294345833633951503673134228239554653750736592122284192", "589aafc0c3de8b80"),
        ("172634793901111761437645980635978698097108221459588354481201172811031581825966870235552637483464550107611589120066973309037240044159245892761959745496885903529251864429585", "6346df2fba9bf7c2"),
        ("165526895961644976991114420297442649881095619259963800498067145652817006591843", "4ff6df4e72e0d224"),
        ("19270013230608649084735106460581905353017328186043497803623643076859879972267705400014129196313500651758207307427", "5740068c103a56a4"),
        ("70889591166011248673", "440ebe535c9c5628"),
    ] {
        let n: num_bigint::BigInt = n.parse().unwrap();
        let got = crate::num::big_to_f64(&n).map(f64::to_bits);
        assert_eq!(got, Some(u64::from_str_radix(bits, 16).unwrap()), "{n}");
    }
    // The float nearest -0.0's text is -0.0, and an underflow keeps its sign.
    assert!(
        matches!(json_decode(b"-1.0e-400"), Ok(Value::Float(f)) if f == 0.0 && f.is_sign_negative())
    );
    assert!(matches!(json_decode(b"-0"), Ok(Value::Int(0))));
}

/// The one bound Erlang does not have: an integer is at most 8192 bits. Text holding a
/// longer one — a payload, a string `int()` converts, a SQL literal — is refused before
/// it is parsed, and a shift past the bound before it is computed, so neither costs
/// more than reading its length.
#[test]
fn huge_integers_are_refused_before_they_cost_anything() {
    let started = std::time::Instant::now();
    let digits = "9".repeat(1_000_000);
    let e = fails(
        "SELECT payload.n AS n FROM \"t/#\"",
        &format!(r#"{{"n":{digits}}}"#),
    );
    assert!(e.contains("integer too large"), "{e}");
    let e = fails(
        "SELECT int(payload.s) AS n FROM \"t/#\"",
        &format!(r#"{{"s":"{digits}"}}"#),
    );
    assert!(e.contains("integer too large"), "{e}");
    let e = fails(
        "SELECT bitsl(1, payload.s) AS n FROM \"t/#\"",
        r#"{"s":1000000000000}"#,
    );
    assert!(e.contains("integer too large"), "{e}");
    assert!(RuleSet::parse(&format!(
        "[rules.r]\nsql = 'SELECT {} AS n FROM \"t\"'\nactions = []\n",
        "9".repeat(3000)
    ))
    .is_err());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );

    // Up to the bound, exact; one bit past it, refused.
    let max: num_bigint::BigInt = (num_bigint::BigInt::from(1) << 8192u32) - 1i32;
    let p = format!(r#"{{"n":{max}}}"#);
    assert_eq!(one("SELECT payload.n AS n FROM \"t/#\"", &p), p);
    assert!(fails("SELECT payload.n + 1 AS n FROM \"t/#\"", &p).contains("integer too large"));
    assert!(
        fails("SELECT payload.n * payload.n AS n FROM \"t/#\"", &p).contains("integer too large")
    );
    assert_eq!(
        one("SELECT -payload.n + 1 - 1 AS n FROM \"t/#\"", &p),
        format!(r#"{{"n":-{max}}}"#)
    );
}

/// Big arithmetic is charged, by size, to the per-message budget every function's
/// output draws on: a FOREACH that multiplies each of a thousand 2,700-bit payload
/// integers fails the rule rather than computing megabytes of them.
#[test]
fn big_arithmetic_is_charged_to_the_message_budget() {
    let x = ((num_bigint::BigInt::from(1) << 2700u32) - 1i32).to_string();
    let sql = "FOREACH payload.a AS x INCASE x * x * x < 0 FROM \"t/#\"";
    let many = |n: usize| format!(r#"{{"a":[{}]}}"#, vec![x.as_str(); n].join(","));
    assert_eq!(
        run_on(sql, "t/a", &many(100)).unwrap(),
        Vec::<String>::new()
    );
    let e = fails(sql, &many(1000));
    assert!(e.contains("budget"), "{e}");
}

/// The JSON reader accepts and refuses exactly what `serde_json` (strict RFC 8259) does,
/// over a seeded corpus of mutated documents — it replaced `serde_json` only to keep big
/// integers exact.
#[test]
fn json_reader_accepts_what_serde_json_accepts() {
    let seeds: &[&[u8]] = &[
        br#"{"a":[1,2.5,-3e2,true,false,null,"x\u00e9\n"],"b":{"c":{}},"d":[]}"#,
        br#"[0,-0,0.0,1E+2,1e-2,"\ud83d\ude00","\"\\\/\b\f\r\t"]"#,
        b"  {\"k\" : \"v\" , \"n\":12345678901234567890}  ",
        "\"caf\u{e9}\"".as_bytes(),
    ];
    let alphabet = b"{}[],:\"\\0123456789.-+eEtrufalsn \t\nxu\x01\xff\xc3\xa9";
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rnd = |n: usize| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        usize::try_from(x % n as u64).unwrap()
    };
    for _ in 0..50_000 {
        let mut b = seeds[rnd(seeds.len())].to_vec();
        for _ in 0..rnd(4) {
            let at = rnd(b.len() + 1);
            let c = alphabet[rnd(alphabet.len())];
            match rnd(3) {
                0 if at < b.len() => {
                    b.remove(at);
                }
                1 if at < b.len() => b[at] = c,
                _ => b.insert(at, c),
            }
        }
        let ours = json_decode(&b).is_ok();
        let serde = serde_json::from_slice::<serde_json::Value>(&b).is_ok();
        assert_eq!(ours, serde, "{:?}", String::from_utf8_lossy(&b));
    }
    // Its errors read as serde's did (docs/RULES.md quotes these).
    for (doc, want) in [
        ("hot", "invalid JSON: expected value at line 1 column 1"),
        (
            "not json",
            "invalid JSON: expected ident at line 1 column 2",
        ),
        (
            "[1,2",
            "invalid JSON: EOF while parsing a list at line 1 column 4",
        ),
        ("01", "invalid JSON: invalid number at line 1 column 2"),
    ] {
        assert_eq!(json_decode(doc.as_bytes()).unwrap_err().to_string(), want);
    }
}
