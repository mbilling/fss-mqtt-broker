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
    assert!(fails("SELECT 9223372036854775807 + 1 as r FROM \"t/#\"", "{}").contains("overflow"));
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
    assert_eq!(val("float('3.14e4')"), "31400.0");
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
    // Binary data is never silently corrupted by JSON encoding.
    assert!(fails("SELECT base64_decode('y0jN') as r FROM \"t/#\"", "{}").contains("binary"));
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
    assert_eq!(val("coalesce(nope, also_nope)"), "null");
}

#[test]
fn load_errors_are_specific() {
    let e = check_sql("SELECT nope(1) FROM \"t\"").unwrap_err();
    assert!(e.contains("unknown function nope()"), "{e}");
    let e = check_sql("SELECT upper() FROM \"t\"").unwrap_err();
    assert!(e.contains("takes 1 argument"), "{e}");
    let e = check_sql("SELECT a FROM \"t/#/x\"").unwrap_err();
    assert!(e.contains("not a valid topic filter"), "{e}");
    let e = check_sql("SELECT a FROM \"$events/message/delivered\"").unwrap_err();
    assert!(e.contains("not a supported event"), "{e}");
    let e = check_sql("SELECT a").unwrap_err();
    assert!(e.contains("expected FROM"), "{e}");
    let e = check_sql("SELECT a FROM \"t\" WHERE a = b = c").unwrap_err();
    assert!(e.contains("do not chain"), "{e}");
    let e = check_sql("SELECT regex_match(a, '(') FROM \"t\"").unwrap_err();
    assert!(e.contains("invalid regular expression"), "{e}");
    let e = check_sql("SELECT a\nFROM \"t\" WHERE a = 'x").unwrap_err();
    assert!(e.contains("line 2"), "{e}");
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
            log.push(format!(
                "{}:{}",
                r.id(),
                match o {
                    Outcome::Passed => "passed".to_string(),
                    Outcome::NoResult => "no_result".to_string(),
                    Outcome::Failed(e) => format!("failed({e})"),
                    Outcome::ActionOk => "action_ok".to_string(),
                    Outcome::ActionFailed(e) => format!("action_failed({e})"),
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
    let (out, _) = effects(&set, &msg("$SYS/x", &payload, &props));
    assert!(out.is_empty(), "leading wildcards never match $-topics");
    let (out, _) = effects(&set, &msg("z", &payload, &props));
    assert!(out.is_empty());
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
    let ev = EventInput::client_connected(&info, 5, 60, true, 0, 1);
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
    let loaded = RuleSet::parse(
        "[rules.r]\nsql = 'SELECT a FROM \"t\"'\nactions = [{ function = \"republish\", args = { topic = \"x\", direct_dispatch = false } }]",
    )
    .unwrap();
    assert!(
        loaded.warnings[0].contains("direct_dispatch"),
        "{:?}",
        loaded.warnings
    );
    assert_eq!(loaded.rules.digest().len(), 64);
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
