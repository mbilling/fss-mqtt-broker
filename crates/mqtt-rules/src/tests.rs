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
    // wrong (but in-range) answer without them.
    let mtr = "SELECT map_to_range(payload.n, payload.lo, payload.hi) AS b FROM \"t/#\"";
    assert_eq!(
        one(mtr, r#"{"n":-5,"lo":-10,"hi":9223372036854775807}"#),
        r#"{"b":9223372036854775803}"#
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
    let mut text = regex_file(&distinct(MAX_REGEX_LITERALS_PER_FILE), 1);
    text.push_str(
        "[rules.dyn]\nsql = 'SELECT regex_match(payload.a, payload.p) AS m FROM \"t\"'\n",
    );
    RuleSet::parse(&text).unwrap_or_else(|e| panic!("{e}"));
}

/// The longest `\w{n}` this build compiles within the per-pattern size limit. The limit is
/// fixed, but what a pattern costs against it depends on the regex features the build
/// unifies: the broker's graph enables `regex-automata/dfa-build` (through
/// tracing-subscriber's env filter), and with it a far shorter run is the largest that
/// fits than in `cargo test -p mqtt-rules` alone.
fn longest_word_run() -> usize {
    (1..=64)
        .rev()
        .find(|n| funcs::compile_regex(&format!("\\w{{{n}}}")).is_ok())
        .expect("\\w compiles")
}

/// An identical pattern is compiled once and counts once, wherever it appears: a file
/// repeating one pattern at the per-pattern size limit far past the budget loads.
#[test]
fn identical_regular_expressions_are_compiled_once() {
    let n = longest_word_run();
    assert!(
        funcs::compile_regex(&format!("\\w{{{}}}", n + 1)).is_err(),
        "\\w{{{n}}} is at the limit"
    );
    let at_limit = format!("\\w{{{n}}}");
    let mut pool = parser::RegexPool::default();
    let first = pool.get(&at_limit).unwrap();
    assert!(Arc::ptr_eq(&first, &pool.get(&at_limit).unwrap()));
    assert!(!Arc::ptr_eq(
        &first,
        &pool.get(&format!("\\w{{{}}}", n - 1)).unwrap()
    ));

    let worst = vec![at_limit; 4 * MAX_REGEX_LITERALS_PER_FILE];
    let set = load(&regex_file(&worst, 32));
    assert_eq!(set.len(), 32);
    // Each still works, and a second distinct pattern is still accepted beside it.
    let mut text = regex_file(&worst, 2);
    text.push_str("[rules.other]\nsql = '''SELECT 1 AS x FROM \"t/#\" WHERE regex_match(payload.a, '^b$')'''\n");
    let set = load(&text);
    let payload = Bytes::from(format!(r#"{{"a":"{}"}}"#, "x".repeat(n)));
    let props = mqtt_core::AppProperties::default();
    let (_, log) = effects(&set, &msg("t/1", &payload, &props));
    assert_eq!(log, ["other:no_result", "r0:passed", "r1:passed"]);
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
    assert_eq!(loaded.warnings.len(), 2, "{:?}", loaded.warnings);
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

/// `statement`'s outputs on a message, evaluated as [`eval::run`] does and as EMQX's
/// SELECT-first order does: `(where_first, select_first)`.
fn both_orders(
    sql: &str,
    topic: &str,
    payload: &str,
) -> (Result<Vec<String>, String>, Result<Vec<String>, String>) {
    let payload = Bytes::from(payload.to_string());
    let props = mqtt_core::AppProperties::default();
    let m = msg(topic, &payload, &props);
    let stmt = compile_alone(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .stmt;
    let render = |outs: Vec<Map>| -> Vec<String> {
        outs.into_iter()
            .map(|o| Value::from(o).to_json().unwrap())
            .collect()
    };
    let mut a = Vec::new();
    let where_first = eval::run(&stmt, &EvalCtx::new(&m), &mut a)
        .map(|()| render(a))
        .map_err(|e| e.to_string());
    let mut b = Vec::new();
    let select_first = eval::run_select_first(&stmt, &EvalCtx::new(&m), &mut b)
        .map(|()| render(b))
        .map_err(|e| e.to_string());
    (where_first, select_first)
}

/// Evaluating the WHERE first, on only the items it reads, gives exactly the outputs (keys,
/// order and values) of EMQX's SELECT-first order on every message that does not fail
/// there; one that does either fails the same way or was turned away first:
/// aliases read by the WHERE, alias chains, dotted aliases, `*`, a WHERE on input fields
/// only, shadowing, and a statement without WHERE.
#[test]
fn where_first_gives_the_select_first_outputs() {
    let statements = [
        r#"SELECT payload.v AS v, clientid FROM "t/#" WHERE v > 2"#,
        r#"SELECT payload.v AS a, a * 2 AS b, b + 1 AS c, upper(clientid) AS who FROM "t/#" WHERE c > 7"#,
        r#"SELECT payload.v AS m.v, payload.w AS m.w, topic FROM "t/#" WHERE m.v > 2"#,
        r#"SELECT *, payload.v AS v FROM "t/#" WHERE v > 2"#,
        r#"SELECT payload.v AS v, *, payload.w AS w FROM "t/#" WHERE w = 'x'"#,
        r#"SELECT payload.v AS v, payload.w AS w FROM "t/#" WHERE clientid = 'c_emqx' AND payload.v > 2"#,
        r#"SELECT payload.v AS clientid FROM "t/#" WHERE clientid > 2"#,
        r#"SELECT payload.v AS v, v AS v FROM "t/#" WHERE v > 2"#,
        r#"SELECT payload.v AS v, CASE WHEN v > 3 THEN 'big' ELSE 'small' END AS size FROM "t/#" WHERE size = 'big'"#,
        r#"SELECT payload.list AS l, nth(2, l) AS second FROM "t/#" WHERE second > 1"#,
        r#"SELECT payload.v AS v, payload FROM "t/#""#,
        r#"SELECT payload.v + 1 FROM "t/#" WHERE payload.v > 2"#,
    ];
    let payloads = [
        r#"{"v": 1, "w": "x", "list": [1, 2, 3]}"#,
        r#"{"v": 5, "w": "y", "list": [4, 0]}"#,
        r#"{"v": 3, "w": "x", "list": [9, 9]}"#,
        r#"{"w": "x"}"#,
    ];
    for sql in statements {
        for p in payloads {
            let (where_first, select_first) = both_orders(sql, "t/a", p);
            match &select_first {
                Ok(_) => assert_eq!(where_first, select_first, "{sql} on {p}"),
                // Where SELECT-first fails, WHERE-first fails the same way, or never
                // evaluated the failing item because the WHERE turned the message away.
                Err(_) => assert!(
                    where_first == select_first || where_first == Ok(Vec::new()),
                    "{sql} on {p}: {where_first:?} vs {select_first:?}"
                ),
            }
        }
    }
}

/// The one difference from EMQX's order: a SELECT item the WHERE does not read is never
/// evaluated for a message the WHERE turns away, so an item that would fail makes that
/// message `no_result` rather than `failed`. A message that passes still fails on it.
#[test]
fn an_item_the_where_does_not_read_cannot_fail_a_message_it_turns_away() {
    let sql = r#"SELECT payload.v AS v FROM "t/#" WHERE clientid = 'someone-else'"#;
    let (where_first, select_first) = both_orders(sql, "t/a", "not json");
    assert_eq!(
        where_first,
        Ok(Vec::new()),
        "turned away, never decoding the payload"
    );
    assert!(
        select_first.is_err(),
        "SELECT-first decodes it and fails: {select_first:?}"
    );

    let sql = r#"SELECT payload.v AS v FROM "t/#" WHERE clientid = 'c_emqx'"#;
    let (where_first, _) = both_orders(sql, "t/a", "not json");
    assert!(
        where_first.is_err(),
        "a message that passes still fails on it: {where_first:?}"
    );
}

/// The plan names exactly the items the WHERE reads, through alias chains, and only items
/// before the one that reads them.
#[test]
fn the_where_plan_follows_alias_chains_backwards_only() {
    let plan = |sql: &str| compile_alone(sql).unwrap().stmt.where_needs;
    assert_eq!(
        plan(
            r#"SELECT payload.v AS a, a * 2 AS b, clientid, b + 1 AS c, payload.x AS d FROM "t/#" WHERE c > 7"#
        ),
        // `payload.x AS d` starts with `payload`, a name `a` reads: computed early, harmlessly.
        Some(vec![true, true, false, true, false]),
    );
    assert_eq!(
        plan(r#"SELECT c * 2 AS b, payload.v AS c FROM "t/#" WHERE b > 1"#),
        Some(vec![true, false]),
        "`b` reads `c` before `c` is selected, so from the trigger: `c` is not needed"
    );
    assert_eq!(
        plan(r#"SELECT payload.v AS v FROM "t/#""#),
        None,
        "no WHERE"
    );
    assert_eq!(
        plan(r#"FOREACH payload.l AS x FROM "t/#" WHERE true"#),
        None,
        "FOREACH keeps its order"
    );
    assert_eq!(
        plan(r#"SELECT payload.v AS v, upper(clientid) AS who FROM "t/#" WHERE topic = 't/a'"#),
        Some(vec![false, false]),
        "a WHERE on the trigger alone computes nothing early"
    );
}
